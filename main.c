#include <u.h>
#include <libc.h>
#include <fcall.h>
#include <thread.h>
#include <9p.h>

#include <webfs.h>
#include <json.h>

enum
{
	DEBUG	= 0,
};

#define dbg(fmt, ...) if(DEBUG) fprint(2, fmt, __VA_ARGS__)

static char *endpoint;

typedef struct EtcdNode EtcdNode;
struct EtcdNode
{
	char 		*key;
	char 		*value;
	int 		dir;
	int 		ttl;
	EtcdNode	**nodes;
	int 		nnodes;
	int 		modifiedidx;
	int			createdidx;
};

static void
etcdnodefree(EtcdNode *e)
{
	int i;
	EtcdNode *n;

	if(e == nil)
		return;

	free(e->key);
	free(e->value);

	if(e->nodes != nil && e->nnodes > 0){
		for(i = 0; i < e->nnodes; i++){
			n = e->nodes[i];
			etcdnodefree(n);
		}
		free(e->nodes);
	}
}

static char *keybase = "/v2/keys";

static EtcdNode*
parsenode(JSON *j)
{
	int i;
	JSON *l, *m;
	JSONEl *e;
	EtcdNode *v;

	v = mallocz(sizeof(*v), 1);
	if(v == nil){
		return nil;
	}

	/* read key */
	l = jsonbyname(j, "key");
	if(l != nil){
		v->key = strdup(jsonstr(l));
	}

	/* version */
	l = jsonbyname(j, "modifiedIndex");
	if(l != nil){
		v->modifiedidx = l->n;
	}

	/* read value */
	l = jsonbyname(j, "value");
	if(l != nil){
		v->value = strdup(jsonstr(l));
	}

	/* read dir */
	l = jsonbyname(j, "dir");
	if(l != nil){
		if(l->t == JSONBool && l->n == 1)
			v->dir = 1;
		l = jsonbyname(j, "nodes");
		if(l != nil && l->t == JSONArray){
			v->nnodes = 0;
			for(e = l->first; e != nil; e = e->next)
				v->nnodes++;

			if(v->nnodes > 0){
				v->nodes = mallocz(v->nnodes * sizeof(EtcdNode*), 1);
				if(v->nodes == nil)
					goto fail;
				i = 0;
				for(e = l->first; e != nil; e = e->next){
					m = e->val;
					v->nodes[i] = parsenode(m);
					if(v->nodes[i] == nil)
						goto fail;
					i++;
				}
			}
		}
	}

	return v;
fail:
	etcdnodefree(v);
	return nil;
}

static EtcdNode*
etcddo(char *ep, char *key, char *opts, char *postbody)
{
	char *data;
	JSON *j, *k;
	Webfs *w;
	EtcdNode *v;

	if(opts == nil)
		opts = "";

	dbg("etcd %s %s %s %s\n", ep, keybase, key, opts);

	w = webfs("%s%s%s%s", ep, keybase, key, opts);
	if(w == nil)
		return nil;

	if(postbody == nil)
		data = webfsget(w);
	else {
		snprint(w->method, sizeof(w->method), "PUT");
		webfshdr(w, "Content-Type", "application/x-www-form-urlencoded");
		data = webfspost(w, postbody);
	}

	webfsfree(w);
	if(data == nil){
		return nil;
	}

	dbg("etcd -> %s\n", data);

	j = jsonparse(data);
	free(data);

	if(j->t != JSONObject){
		jsonfree(j);
		return nil;
	}

	k = jsonbyname(j, "message");
	if(k != nil){
		werrstr("%s", jsonstr(k));
		jsonfree(j);
		return nil;
	}

	k = jsonbyname(j, "node");
	if(k == nil){
		jsonfree(j);
		return nil;
	}

	v = parsenode(k);
	jsonfree(j);
	return v;
}

uvlong jenkinshash(char *key, int len)
{
	u32int hash, i;
	for(hash = i = 0; i < len; ++i){
		hash += key[i];
		hash += (hash << 10);
		hash ^= (hash >> 6);
	}
	hash += (hash << 3);
	hash ^= (hash >> 11);
	hash += (hash << 15);
	return (uvlong)hash;
}

static char*
unslash(char *s)
{
	char *r;

	r = strrchr(s, '/');
	if(r == nil)
		return s;
	return r+1;
}

typedef struct Aux Aux;
struct Aux
{
	char *path;
	EtcdNode *node;
};

void
aux2qid(Aux *a, Qid *q)
{
	q->type = 0;
	q->vers = a->node->modifiedidx;
	q->path = jenkinshash(a->path, strlen(a->path));

	if(a->node->dir != 0 || strcmp(a->path, "/") == 0)
		q->type = QTDIR;

	dbg("aux2qid %s -> %08llux %lud %hhux\n", a->path, q->path, q->vers, q->type);
}

static void
fsdestroyfid(Fid *fid)
{
	Aux *a;

	a = fid->aux;

	if(a == nil)
		return;

	free(a->path);
	etcdnodefree(a->node);
	free(a);
}

static void
fsattach(Req *r)
{
	char *spec;
	EtcdNode *n;
	Aux *a;

	spec = r->ifcall.aname;
	if(spec && spec[0]){
		respond(r, "invalid attach specifier");
		return;
	}

	a = emalloc9p(sizeof(*a));
	a->path = estrdup9p("/");
	a->node = etcddo(endpoint, a->path, nil, nil);
	if(a->node == nil){
		respond(r, "attach failed");
		return;
	}

	r->fid->aux = a;
	aux2qid(a, &r->ofcall.qid);
	r->fid->qid = r->ofcall.qid;
	respond(r, nil);
}

static char*
fsclone(Fid *ofid, Fid *fid)
{
	EtcdNode *n;
	Aux *old, *new;

	old = ofid->aux;

	dbg("fsclone %s\n", old->path);

	n = etcddo(endpoint, old->path, nil, nil);
	if(n == nil)
		return "clone failed";

	new = emalloc9p(sizeof(*new));
	new->path = strdup(old->path);
	new->node = n;

	fid->aux = new;
	return nil;
}

static char*
fswalk1(Fid *fid, char *name, Qid *qid)
{
	int dotdot;
	char *p, buf[256];
	EtcdNode *n;
	Aux *a;

	dotdot = strcmp(name, "..") == 0;

	a = fid->aux;

	if(dotdot){
		strcpy(buf, a->path);
		p = strrchr(buf, '/');
		if(p  != nil){
			*p = 0;
		}
	} else if(strcmp(a->path, "/") == 0)
		snprint(buf, sizeof(buf), "/%s", name);
	else
		snprint(buf, sizeof(buf), "%s/%s", a->path, name);

	dbg("fswalk1 %s %s -> %s \n", a->path, name, buf);

	n = etcddo(endpoint, buf, nil, nil);
	if(n == nil){
		//snprint(buf, sizeof(buf), "etcdget: %r");
		//respond(r, buf);
		return "file does not exist";
	}

	free(a->path);
	etcdnodefree(a->node);

	a->path = estrdup9p(buf);
	a->node = n;

	aux2qid(a, qid);
	fid->qid = *qid;

	dbg("fswalk1 %s\n", buf);

	return nil;
}

static void
fsopen(Req *r)
{
	respond(r, nil);
}

static int dirgen(int i, Dir *d, void *v)
{
	EtcdNode *n, *e;
	char *name;
	Aux *a, tmp;

	a = v;
	n = a->node;

	memset(d, 0, sizeof(*d));
	d->uid = estrdup9p("etcd");
	d->gid = estrdup9p("etcd");
	d->mode = 0777;
	d->atime = d->mtime = 0; //time(nil);

	if(i >= n->nnodes)
		return -1;

	e = n->nodes[i];

	if(e->dir){
		d->mode |= DMDIR;
		d->length = 0;
	}

	if(e->value != nil)
		d->length = strlen(e->value);

	d->name = estrdup9p(unslash(e->key));

	dbg("dirgen name %s dname %s\n", e->key, d->name);

	tmp.path = e->key;
	tmp.node = e;

	aux2qid(&tmp, &d->qid);

	return 0;
}

static void
fsread(Req *r)
{
	int i;
	char buf[256];
	EtcdNode *n;
	Aux *a, tmp;

	a = r->fid->aux;

	n = etcddo(endpoint, a->path, nil, nil);
	if(n == nil){
		/* XXX: buf is on stack? */
		snprint(buf, sizeof(buf), "etcd: %r");
		respond(r, buf);
		return;
	}

	if(!n->dir){
		readstr(r, n->value);
	} else {
		tmp.path = a->path;
		tmp.node = n;
		dirread9p(r, dirgen, &tmp);
	}

	etcdnodefree(n);
	respond(r, nil);
}

static void
fswrite(Req *r)
{
	char buf[8192+1];
	EtcdNode *n;
	Aux *a;

	a = r->fid->aux;

	snprint(buf, sizeof(buf), "value=%.*s", r->ifcall.count, r->ifcall.data);
	
	n = etcddo(endpoint, a->path, nil, buf);
	if(n == nil){
		dbg("etcd post %s: %r\n", a->path);
		respond(r, "post failed");
		return;
	}

	etcdnodefree(a->node);
	a->node = n;

	r->ofcall.count = r->ifcall.count;
	respond(r, nil);
}

void
fscreate(Req *r)
{
	respond(r, nil);
}

static void
fsstat(Req *r)
{
	Dir *d;
	Aux *a;

	d = &r->d;
	a = r->fid->aux;

	memset(d, 0, sizeof(*d));
	d->uid = estrdup9p("etcd");
	d->gid = estrdup9p("etcd");
	d->atime = d->mtime = 0; //time(nil);

	d->mode = 0777;

	if(strcmp(a->path, "/") == 0){
		d->name = estrdup9p("/");
		d->mode |= DMDIR;
	} else {
		d->name = estrdup9p(unslash(a->path));
	}

	if(a->node != nil && a->node->dir)
		d->mode |= DMDIR;

	aux2qid(a, &d->qid);

	respond(r, nil);
}

Srv fs =
{
.destroyfid	= fsdestroyfid,
.attach		= fsattach,
.clone		= fsclone,
.walk1		= fswalk1,
.open		= fsopen,
.read		= fsread,
.write		= fswrite,
.create		= fscreate,
.stat		= fsstat,
};

void
usage(void)
{
	fprint(2, "usage: %s [-D] [-m mntpt] [-s service] -e endpoint\n", argv0);
	exits("usage");
}

void
main(int argc, char *argv[])
{
	int i;
	char *srvpoint, *mntpoint;
	EtcdNode *r, *e;

	quotefmtinstall();

	srvpoint = "etcd";
	mntpoint = "/n/etcd";

	ARGBEGIN{
	case 'D':
		chatty9p++;
		break;
	case 'm':
		mntpoint = EARGF(usage());
		break;
	case 's':
		srvpoint = EARGF(usage());
		break;
	case 'e':
		endpoint = EARGF(usage());
		break;
	default:
		usage();
	}ARGEND

	if(endpoint == nil)
		sysfatal("missing endpoint");

	postmountsrv(&fs, srvpoint, mntpoint, MREPL | MCREATE);

	exits(nil);
}

export class Router {
    constructor() { this.routes = []; }
    add(method, path, handler) {
        const norm = this.#normalize(path);
        this.routes.push({ method: method.toUpperCase(), path: norm, handler });
        return this;
    }
    async handle(request, env) {
        const url = new URL(request.url);
        const method = request.method.toUpperCase();
        const pathname = this.#normalize(url.pathname);
        const route = this.routes.find(r => r.method === method && r.path === pathname);
        if (!route) return null;
        return await route.handler(request, env);
    }
    #normalize(p) {
        if (!p) return '/';
        const noTrail = p.replace(/\/+$/, '');
        return noTrail === '' ? '/' : noTrail;
    }
}



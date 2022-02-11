import axios from "axios";
export default class Database {
    #instance;
    #meta;
    constructor(url = "http://localhost:8080", { user, password } = {}) {
        return (async () => {
            this.#instance = axios.create({
                baseURL: url,
            });
            if (user && password) {
                let token = await this.#instance
                    .post("/auth", {
                        user,
                        password,
                    })
                    .then((res) => res.data);
                this.#instance.defaults.headers.common[
                    "Authorization"
                ] = `Bearer ${token}`;
            }
            await this.start();
            return this;
        })();
    }
    async start() {
        let meta = await this.#instance.get("/meta").then((res) => res.data);
        this.#meta = meta;
    }

    async close() {}

    get meta() {
        return this.#meta;
    }

    table(name) {
        if (!name) {
            throw new Error("no tableName provided");
        }
        if (!this.meta.tables[name]) {
            throw new Error("table does not exist");
        }
        return new Table(this.meta.tables[name], this.#instance);
    }
}

class Table {
    #meta;
    #instance;
    constructor(meta, instance) {
        this.#meta = meta;
        this.#instance = instance;
    }

    get meta() {
        return this.#meta;
    }

    async get(id) {
        return this.#instance
            .get(`/table/${this.meta.name}/${id}`)
            .then((res) => res.data);
    }

    async ensureIndex(name) {
        throw new Error("not implemented");
    }

    async removeIndex(name) {
        throw new Error("not implemented");
    }

    async find(query, context = {}) {
        let result = await this.#instance
            .post(`/table/${this.meta.name}/`, {
                query: query.toString(),
                context,
            })
            .then((res) => res.data);
        return result;
    }

    filter(func, context = {}) {
        return new Query(func, context, this.meta.name, this.#instance);
    }

    async save(data) {
        let id = await this.#instance
            .put(`/table/${this.meta.name}/`, data)
            .then((res) => res.data);
        return id;
    }

    async remove(id) {
        return this.#instance
            .delete(`/table/${this.meta.name}/${id}`)
            .then((res) => res.data);
    }
}

class Query {
    #tableName;
    #instance;
    #actions = [];

    constructor(query, context, tableName, instance) {
        this.#actions.push({
            type: actionTypes.FILTER,
            data: { query: query.toString(), context },
        });
        this.#tableName = tableName;
        this.#instance = instance;
    }

    filter(query, context) {
        this.#actions.push({
            type: actionTypes.FILTER,
            data: { query: query.toString(), context },
        });
        return this;
    }
    sort(query, context) {
        this.#actions.push({
            type: actionTypes.SORT,
            data: { query: query.toString(), context },
        });
        return this;
    }
    map(query, context) {
        this.#actions.push({
            type: actionTypes.MAP,
            data: { query: query.toString(), context },
        });
        return this;
    }
    reduce(query, initVal, context) {
        this.#actions.push({
            type: actionTypes.REDUCE,
            data: { query: query.toString(), context, initVal },
        });
        return this;
    }

    async then(resolve, error) {
        let result = await this.#instance
            .post(`/table/${this.#tableName}/`, {
                type: "chain",
                chain: this.#actions,
            })
            .then((res) => res.data);
        resolve(result);
    }
}

export let actionTypes = {
    FILTER: 0,
    SORT: 1,
    MAP: 2,
    REDUCE: 3,
};

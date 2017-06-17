export class $ extends Object {
    static values (obj : Object) {
        return Object.keys(obj).map(e => obj[e]);
    }

    static count (obj : Object, s : string) {
        return Object.keys(obj).filter(e => { if (e.indexOf(s) === 0) return e }).length;
    }

    static getKey(obj : Object, v : any) {
        for (let k in obj) {
            if (obj[k] === v) return k;
        }

        return null;
    }
}
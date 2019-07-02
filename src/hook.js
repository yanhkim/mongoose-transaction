'use strict';
require('songbird');

const PRE_POST_HOOK_TYPES = ['commit', 'expire'];

class Hook {
    constructor(once = false) {
        this.once = once;
        this.hooks = {
            finalize: [],
        };
        PRE_POST_HOOK_TYPES.forEach((type) => {
            this.hooks['pre_' + type] = [];
            this.hooks['post_' + type] = [];
        });
    }

    _addHook(types, hook) {
        const group = (Array.isArray(types) ? types : [types]).join('_');
        // not supported this.
        if (!Array.isArray(this.hooks[group])) {
            return;
        }
        this.hooks[group].push(hook);
        // TODO: unique
    }

    async doHooks(context, ...types) {
        const group = types.join('_');
        if (!this.hooks || !this.hooks[group] || !this.hooks[group].length) {
            return;
        }

        if (this.once) {
            if (this.hooks[group].called) {
                return;
            }
            // prevent double call
            this.hooks[group].called = true;
        }

        const promises = this.hooks[group].map(async(f) => {
            try {
                await f(context);
            } catch(e) {
                const msg = e.stack || e.message || e.toString();
                process.stderr.write(msg + '\n');
            }
        });
        await Promise.all(promises);
    }

    pre(type, hook) {
        this._addHook(['pre', type], hook);
    }

    post(type, hook) {
        this._addHook(['post', type], hook);
    }

    finalize(hook) {
        this._addHook('finalize', hook);
    }

    clone(once = true) {
        const hook = new Hook(once);
        Object.keys(this.hooks).forEach((key) => {
            hook.hooks[key] = this.hooks[key].slice();
        });
        return hook;
    }
};

module.exports = Hook;

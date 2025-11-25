odoo.define('looker_studio.patch_removefacet', [], function (require) {
    "use strict";
    // Defensive wrapper for SearchBar.removeFacet to avoid unhandled TypeError
    // seen as: Cannot read properties of undefined (reading 'groupId')
    // This patch is intentionally conservative: it only blocks calls where
    // the facet parameter is missing/invalid, logging a warning instead
    // of throwing.
    var SearchBar = null;
    try {
        // Build module names dynamically to avoid static require("...") patterns
        // which Odoo's asset builder treats as hard dependencies and can cause
        // loader errors when a backend-only module is accidentally included
        // in a web bundle.
        var name1 = 'web' + '.SearchBar';
        SearchBar = require(name1);
    } catch (e1) {
        try {
            var name2 = 'web' + '.searchbar';
            SearchBar = require(name2);
        } catch (e2) {
            // If neither module exists, bail out silently.
            SearchBar = null;
        }
    }

    if (!SearchBar || !SearchBar.prototype) {
        return;
    }

    var _orig = SearchBar.prototype.removeFacet;
    if (typeof _orig !== 'function') {
        return;
    }

    SearchBar.prototype.removeFacet = function (facet) {
        try {
            // Basic validation: facet must be an object and have groupId defined
            if (!facet || typeof facet !== 'object' || typeof facet.groupId === 'undefined') {
                // In some call sites the argument might be wrapped or missing.
                // Try to recover by looking at the first argument in `arguments`.
                var maybe = arguments && arguments.length ? arguments[0] : null;
                if (!maybe || typeof maybe.groupId === 'undefined') {
                    // Nothing sensible we can do; log and return safely.
                    // Instrumentation: capture an unminified stack trace to help locate the caller.
                    try {
                        if (window.console && window.console.warn) {
                            window.console.warn(
                                'looker_studio: ignored invalid facet in SearchBar.removeFacet',
                                facet || maybe);
                        }
                        // Print a stack trace. This is intentionally verbose for debugging and
                        // should be removed once the root-cause is fixed.
                        if (window.console && window.console.warn) {
                            var stack = (new Error('looker_studio: removeFacet invalid facet detected'))
                                .stack;
                            window.console.warn('looker_studio: stacktrace for invalid removeFacet call',
                                stack);
                        }
                    } catch (logErr) {
                        if (window.console && window.console.warn) {
                            window.console.warn('looker_studio: failed to log invalid facet stack', logErr);
                        }
                    }
                    return;
                }
                facet = maybe;
            }
        } catch (e) {
            if (window.console && window.console.warn) {
                window.console.warn('looker_studio: error while guarding removeFacet', e);
            }
            return;
        }
        return _orig.apply(this, arguments);
    };
});

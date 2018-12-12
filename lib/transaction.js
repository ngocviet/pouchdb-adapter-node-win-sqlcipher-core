'use strict';

var events = require('events').EventEmitter;

var prohibitedMethods = [
  'emit', 'addListener', 'setMaxListeners',
  'on', 'once', 'removeListener',
  'removeAllListeners', 'listeners',
  'prepare'
];

var lockMethods = {
  'exec': true,
  'run': true,
  'get': true,
  'all': true,
  'each': true,
  'map': true,
  'finalize': true,
  'reset': true
};

function TransactionDatabase(opts, callback) {
    var transdb = this;
    var queue = [];
    var _lock = 0;
    var db = opts.database;
    var currentTransaction = null;
    var _exec = null;
    db.serialize();

    if (opts.exec && (opts.exec instanceof Function)) {
        _exec = function() {
            return opts.exec(db);
        };
    } else {
        _exec = db.exec.bind(db);
    }

    wrapObject(transdb, transdb, transdb.db);

    this.db.on('error', function() {
        if (this.currentTransaction != null) {
            this.currentTransaction.rollback(function() {
            });
        }
    });

    /**
     * Wrap for prepare method. Return the wrapped object
     */
    function prepare() {
        var oldStatement = db.prepare.apply(db, arguments);
        var newStatement = new events.EventEmitter();
        wrapObject(transdb, newStatement, oldStatement);
        return newStatement;
    }

    /**
     * Wait until the lock is released.
     * Using setTimeout to check the lock recursively
     * The callback is called when the lock is released.
     */
    function _wait(callback) {
        function check() {
            if (transdb._lock === 0) {
                callback();
            } else {
                setTimeout(check, 1);
            }
        }
        check();
    }

    /**
     * Execute waiting items in the queue.
     * The item can be a beginTransaction or a statement to underlying database.
     * beginTransaction starts the statement and pauses the execution of the queue.
     * Statements are executed without waiting for them to finish
     * because they were added in parallel, so it's not the problem.
     *
     */
    function _runQueue() {
        while (transdb.queue.length > 0) {
            var item = transdb.queue.shift();
            if (item.type == 'lock') {
                transdb._lock ++;
            }

            item.object[item.method].apply(item.object, item.args);

            // Stop the queue and return if the item is 'beginTransaction'
            if (item.type == 'transaction')
                return;
        }
    }

    function beginTransaction(callback) {
        var self = transdb;

        if (!(self.currentTransaction == null)) {
            self.queue.push({
                type: 'transaction',
                object: self,
                method: 'beginTransaction',
                args: arguments
            });
            return;
        }

        // prepare the transaction object
        var tr = self.db;
        var finished = false;
        self.currentTransaction = tr;

        function finishTransaction(e, cb1) {
            finished = true;
            self.currentTransaction = null;
            self._runQueue();
            cb1(e);
        }

        tr.commit = function(cb) {
            if (finished) return cb(new Error('Transaction is already commit and cannot do commit().'));

            // wait until the lock is released and commit the transaction
            self._wait(function(err) {
                if (err) callback(err);
                self._exec('COMMIT;', function(err) {
                    if (err) {
                        // There's error in the commit process, we need to rollback and callback with error as parameter
                        tr.rollback(function() {
                            cb(error);
                        });
                    } else {
                        finishTransaction(null, cb);
                    }
                });
            });
        };

        tr.rollback = function(cb) {
            if (finished) return cb(new Error('Transaction is already finished and cannot rollback().'));
            self._wait(function(err) {
                if (err) callback(err);
                self._exec('ROLLBACK;', function(err) {
                    finishTransaction(err, cb);
                });
            });
        }

        // begin the transaction
        self._wait(function(err) {
            if (err) finishTransaction(err, callback);
            self._exec('BEGIN;', function(err) {
                if (err) return callback(err);
                callback(null, tr);
            });
        });
    }

    /**
     * Wrap the transaction database with new object
     */
    function wrapObject(transactionDatabase, target, source) {
        for (var method in source) {
            if ((source[method] instanceof Function) && (prohibitedMethods.indexOf(method) < 0)) {
                target[method] = wrapMethod(transactionDatabase, source, method);
            }
        }

        events.EventEmitter.call(target);
        interceptEmit(target, source, target.emit.bind(target));; // pass through all events
    }

    /**
     * Delay the method if we're currently in a transaction
     */
    function wrapDbMethod(transactionDatabase, object, method) {
        function dummyCallback(e) {
            if (e) transactionDatabase.db.emit('error', e);
        }

        var locking = lockMethods.hasOwnProperty(method);

        return function() {
            var args = arguments;
            
            // If needed add a dummy completion callback to 'each' method so that
            // the callback wrapper can decrease the lock value
            if (locking) {
                if (method == 'each') {
                    if (arguments.length <= 2 ||
                        !(args[args.length -1] instanceof Function) &&
                        !(args[args.length -2] instanceof Function)) {
                            // Set the "callback" and "complete" callback to dummy method
                            args[args.length] = args[args.length + 1] = dummyCallback;
                            args.length += 2;
                    } else if ((args[args.length - 1] instanceof Function) && !(args[args.length -2] instanceof Function)) {
                        // Set the "complete" callback to dummy method
                        args[args.length] = dummyCallback;
                        args.length ++;
                    }
                }

                // hijack the callback to implement locking
                var originalCallback = undefined;
                var newCallback = function() {
                    if (transactionDatabase._lock < 1) throw new Error('Locks are not balanced');
                    transactionDatabase._lock--;
                    originalCallback.apply(transdb, arguments);
                }
                // assign the hijacked callback to the arguments of methods
                if (arguments.length > 0 && args[args.length - 1] instanceof Function) {
                    originalCallback = args[args.length - 1];
                    args[args.length - 1] = newCallback;
                } else {
                    // assign to dummy callback if there's no callback
                    originalCallback = dummyCallback;
                    args[args.length] = newCallback;
                    args.length ++;
                }
            }

            if (transactionDatabase.currentTransaction == null) {
                // There's no current transaction
                if (locking) {
                    // lock the method, increase the _lock  and then execute the method
                    // the lock will be released (decrease) before the callback is applied
                    transactionDatabase._lock++;
                }
                object[method].apply(object, args);
            } else {
                // There's current transaction, push the method to the queue
                transactionDatabase.queue.push({
                    type: locking ? 'lock': 'simple',
                    object: object,
                    method: method,
                    args: args
                });
            }
        }
    }
    
    /**
     * Intercept all events from emitter to inject the handler
     */
    function interceptEmit(self, emitter, handler) {
       var oldEmit = emitter.emit;
       emitter.emit = function() {
           handler.apply(self, arguments);
           return oldEmit.apply(emitter, arguments);
       }
   }
}

module.exports = TransactionDatabase;

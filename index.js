var redis = require('redis'),
    Step = require('step'),
    minimist = require('minimist'),
    mainLogLevel = 0;

function debug(text, logLevel) {
    logLevel = logLevel || 4;
    if (logLevel <= mainLogLevel) {
        console.log(text);
    }
}

function Worker(params) {
    this.id = params.id || 0;
    this.client = null;
    this.expireInterval = params.expireInterval || 2;
    this.publishInterval = (params.publishInterval === undefined) ? 500 : params.publishInterval;
    this.isPublisher = false;
    this.needToStop = false; //Flag to stop if an error occured
    this.limitMessages = (params.limitMessages === undefined) ? -1 : (params.limitMessages + 1);

    this.stats = {
        received: 0,
        published: 0
    };
}

Worker.prototype.subscribe = function() {
    var self = this;

    var eventHandler = function(msg, callback) {
        function onComplete() {
            var error = Math.random() > 0.85;
            callback(error, msg);
        }

        setTimeout(onComplete, Math.floor(Math.random() * 1000));
    };

    var errorHandler = function(err, msg) {
        if (err) {
            self.client.rpush('errors', msg, function(error, asnw) {
                if (error) {
                    self.needToStop = true;
                    throw new Error(error);
                }
            });
        }

        self.cnt = ++msg;
        process.nextTick(receiveMes);
    };

    var receiveMes = function() {
        if (!self.isPublisher && !self.needToStop) {
            self.client.lpop('messages', function(err, result) {
                if (err) {
                    debug('Caught an error from redis: ' + err.message, 1);
                    self.needToStop = true;
                }

                if (!result) {
                    process.nextTick(receiveMes);
                } else {
                    debug('A message has been received: ' + result, 3);
                    ++self.stats.received; //for statistic
                    eventHandler(result, errorHandler);
                }
            });
        }
    };

    receiveMes();
};

Worker.prototype.becomePublisher = function(callback) {
    var self = this,
        checkPublishInterval = this.expireInterval * 1000;

    (function tryToBecomePub() {
        if (!self.isPublisher && !self.needToStop) {
            self.client.set('publisher', 1, 'NX', 'EX', self.expireInterval, function(err, res) {
                if (err) {
                    self.needToStop = true;
                    callback && callback(err);
                }

                if (res === 'OK') {
                    self.isPublisher = true;
                    debug('Became a publisher', 1);
                    callback && callback(null, true);
                } else {
                    setTimeout(tryToBecomePub, checkPublishInterval);
                }
            });
        }
    })();
};

Worker.prototype.remainPublisher = function(callback) {
    var self = this,
        remainInterval = this.expireInterval * 500;

    (function capturePubFlag() {
        if (!self.needToStop) {
            self.client.set('publisher', true, 'EX', self.expireInterval, function(err, res) {
                if (err) {
                    debug('error message: ' + err.message, 1);
                    self.needToStop = true;
                    throw new Error(err);
                }

                if (res === 'OK') {
                    debug('We are still a publisher', 2);
                    setTimeout(capturePubFlag, remainInterval);
                } else {
                    debug('unknown answer: ' + res);
                    self.needToStop = true;
                    callback(null, {
                        stop: true
                    });
                }
            });
        } else {
            callback(null, {
                stop: true
            });
        }
    })();
};

Worker.prototype.getMessage = function() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
};

Worker.prototype.startPublish = function(callback) {
    var self = this;

    (function sendMes() {
        if (!self.needToStop) {
            if (--self.limitMessages !== 0) {
                self.client.rpush('messages', self.getMessage(), function(err, answ) {
                    if (err) {
                        self.needToStop = true;
                        throw new Error(err);
                    } else {
                        debug('Just publish a message', 3);
                        ++self.stats.published; //for statistic
                        setTimeout(sendMes, self.publishInterval);
                    }
                });
            } else {
                debug('Stop publish messages', 2);
                self.needToStop = true;
                callback(null, {
                    stop: true
                });
            }
        } else {
            callback(null, {
                stop: true
            });
        }
    })();
};

Worker.prototype.enableClient = function(callback) {
    this.client = redis.createClient();

    this.client.on('error', function(err) {
        throw new Error(err);
    });

    this.client.on('ready', function() {
        callback(null);
    });
};

Worker.prototype.printStats = function() {
    var self = this;

    var intHND = setInterval(function() {
        console.log('Worker-' + self.id + ': ' + JSON.stringify(self.stats, null, 4));
        if (self.needToStop) {
            clearInterval(intHND);
        }
    }, 5000);
};

Worker.prototype.printErrors = function(callback) {
    var self = this;

    Step(function() {
        self.enableClient(this.parallel());
    }, function(err, res) {
        if (err) throw err;

        var bulk = self.client.multi();
        bulk.lrange('errors', 0, -1);
        bulk.del('errors');

        bulk.exec(function(err, answ) {
            if (err) throw err;

            console.log('Errors: ', answ[0]);
            self.client.quit();
            callback && callback({
                res: 'ok'
            });
        });
    }, function(err) {
        debug('Caught an error: ' + err.message, 1);
        self.client.quit();
        callback && callback({
            res: 'err',
            descr: err.message
        });
    });
};

Worker.prototype.start = function(callback) {
    var self = this;

    Step(function() {
        self.enableClient(this.parallel());
    }, function(err, res) {
        if (err) throw err;

        self.subscribe();
        self.becomePublisher(this.parallel());
        self.printStats();
    }, function(err, res) {
        if (err) throw err;

        var group = this.group();

        self.remainPublisher(group());
        self.startPublish(group());
    }, function(err, answers) {
        if (err) throw err;

        self.client.quit();
        return;
    }, function(err) {
        self.client.quit();

        callback && callback({
            res: 'err',
            descr: err.message
        });
    });
};

var inputs = minimist(process.argv);

if (inputs.debug) {
    mainLogLevel = +inputs.debug;
}

if (inputs.getErrors) {
    debug('Before print errorrs', 0);
    var curWorker = new Worker({
        id: 0
    });
    curWorker.printErrors();
} else if (inputs.test) {
    debug('Start benchmark', 0);
    for (var i = 0; i < 5; ++i) {
        var curWorker = new Worker({
            id: i,
            publishInterval: 0,
            limitMessages: 1000000
        });
        curWorker.start();
    }
} else {
    debug('Starting single worker', 0);
    var curWorker = new Worker({
        id: 0
    });
    curWorker.start();
}
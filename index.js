var redis = require('redis'),
	minimist = require('minimist'),
	_ = require('underscore'),
	mainLogLevel = 0;

function debug(text, logLevel) {
	logLevel = logLevel || 4;
	if (logLevel <= mainLogLevel) {
		console.log(text);
	}
}

function Writer(params) {
	params = _.extend({}, params);
	params.publishInterval = (params.publishInterval === null || params.publishInterval === undefined) ? 500 : params.publishInterval;

	this.client = redis.createClient();
	this.expireInterval = params.expireInterval || 2;
	this.publishInterval = params.publishInterval;
	this.limitMessages = params.limit || -1;
	this.unsibscribe = false;

	this.becomeHND = null;
	this.remainHND = null;
	this.publishHND = null;
	this.receivedAmount = 0;

	var self = this;
}

Writer.prototype.becomePublisher = function(callback) {
	var self = this,
		checkPublishInterval = this.expireInterval  * 1000;

    this.becomeHND = setInterval(function() {
        self.client.set('publisher', 1, 'NX', 'EX', self.expireInterval, function(err, res) {
            if (err) {
                clearInterval(self.becomeHND);
                callback && callback(err);
            }

            if (res === 'OK') {
                clearInterval(self.becomeHND);
                debug('Became a publisher', 1);
                callback && callback(null, true);
            }
        });
    }, checkPublishInterval);
};

Writer.prototype.remainPublisher = function() {
    var self = this,
    	remainInterval = this.expireInterval * 500;

    this.remainHND = setInterval(function() {
        self.client.set('publisher', true, 'EX', self.expireInterval, function(err, res) {
            if (err) {
                debug('error message: ' + err.message, 1);
                clearInterval(self.remainHND);
                throw new Error(err.message);
            }

            if (res === 'OK') {
                debug('We are still a publisher', 2);
            }
        });
    }, remainInterval);
};

Writer.prototype.getMessage = function() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
};

Writer.prototype.subscribe = function() {
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
				if (error) throw new Error(error);
			});
		}

		self.cnt = ++msg;
		process.nextTick(receiveMes);
	};

	var receiveMes = function() {
		if (!self.unsibscribe) {
			self.client.lpop('messages', function(err, result) {
				if (err) {
					debug('Caught an error from redis: ' + err.message, 1);
					throw new Error(err);
				}

				if (!result) {
					process.nextTick(receiveMes);
				} else {
					debug('A message has been received: ' + result, 3);
					++self.receivedAmount;//for statistic
					eventHandler(result, errorHandler);
				}
			});
		}
	};

	receiveMes();
};

Writer.prototype.startPublish = function() {
    var self = this;
    this.unsibscribe = true;

    this.publishHND = setInterval(function() {
    	if (--self.limitMessages !== 0) {
    		self.client.rpush('messages', self.getMessage(), function(err, answ) {
    			if (err) throw new Error(err);
    		});
    	} else {
    		clearInterval(self.publishHND);
    		debug('Stop publish messages', 2);
    	}
    }, this.publishInterval);
};

Writer.prototype.printErrors = function(callback) {
	var bulk = this.client.multi();
	bulk.lrange('errors', 0, -1);
	bulk.del('errors');

	bulk.exec(function(err, answ) {
		if (err) {
			callback && callback({
				res : 'err',
				descr : err.message
			});
		}
		else {
			console.log(answ[0]);
			callback && callback({
				res : 'ok'
			});
		}
	});
};

Writer.prototype.getReceivedAmmount = function() {
	return this.receivedAmount;
};

Writer.prototype.terminate = function() {
	this.unsibscribe = true;
	this.becomeHND && clearInterval(this.becomeHND);
	this.remainHND && clearInterval(this.remainHND);
	this.publishHND && clearInterval(this.publishHND);
	this.client.quit();
};

function Worker(params) {
	params = _.extend({}, params);
	this.workerId = params.id;
    this.writer = new Writer({
    	limit : params.limit || -1,
    	expireInterval : 2,
    	publishInterval : params.publishInterval
    }); 
}

Worker.prototype.printErrors = function() {
	var self = this;
	this.writer.printErrors(function() {
		self.writer.terminate();
	});
};

Worker.prototype.makePublisherStuff = function(callback) {
    debug('Start doing publisher\'s work', 3);
    try {
    	this.writer.startPublish();
    	this.writer.remainPublisher();
    }
    catch(e) {
    	callback && callback({
    		res : 'err',
    		descr : e.message
    	});
    }
};

Worker.prototype.start = function() {
	var self = this,
		showStatHND = null;
	try {
		this.writer.subscribe();
		showStatHND = setInterval(function() {
			debug('-------------------------------', 1);
			debug('Worker-' + self.workerId + ' received ' + self.writer.getReceivedAmmount() + ' messages', 1);
			debug('-------------------------------\n', 1);
		}, 5000);
	}
	catch(e) {
		debug('Handle an error: ' + e.message, 1);
		return;
	}

	this.writer.becomePublisher(function(err, resp) {
		showStatHND && clearInterval(showStatHND);
        if (err) {
            debug('An error has handled: ' + err.message, 1);
            this.writer.terminate();
            return;
        } else {
            if (resp === true) {
                self.makePublisherStuff();
            } else {
                debug('unknown paramenter: ' + resp, 1);
                this.writer.terminate();
                return;
            }
        }
    });
};

var inputs = minimist(process.argv);

if (inputs.debug) {
	mainLogLevel = +inputs.debug;
}

if (inputs.getErrors) {
	debug('Before print errorrs', 0);
	var curWorker = new Worker();
	curWorker.printErrors();
}
else if (inputs.test) {
	debug('Start benchmark', 0);
	for (var i = 0; i < 5; ++i) {
		var curWorker = new Worker({
			id: i,
			publishInterval : 0,
			limit : 1000000
		});
    	curWorker.start();
	}
}
else {
	debug('Starting single worker', 0);
	var curWorker = new Worker({
		id : 0
	});
    curWorker.start();
}

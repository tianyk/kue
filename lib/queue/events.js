/*!
 * kue - events
 * Copyright (c) 2013 Automattic <behradz@gmail.com>
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var redis = require('../redis');

/**
 * Job map.
 */

exports.jobs = {};

/**
 * Pub/sub key.
 */

exports.key = 'events';

/**
 * Add `job` to the jobs map, used
 * to grab the in-process object
 * so we can emit relative events.
 *
 * @param {Job} job
 * @api private
 */
exports.callbackQueue = [];


/**
 *
 * @param {[type]}   job      [description]
 * @param {Function} callback [description]
 */
exports.add = function(job, callback) {
    if (job.id) exports.jobs[job.id] = job;
    //  if (!exports.subscribed) exports.subscribe();
    if (!exports.subscribeStarted) exports.subscribe();
    if (!exports.subscribed) {
        exports.callbackQueue.push(callback);
    } else {
        callback();
    }
};


/**
 * Remove `job` from the jobs map.
 *
 * @param {Job} job
 * @api private
 */
exports.remove = function(job) {
    delete exports.jobs[job.id];
};


/**
 * Subscribe to "q:events".
 *
 * @api private
 */
exports.subscribe = function() {
    //  if (exports.subscribed) return;
    if (exports.subscribeStarted) return; // 是否已经订阅
    var client = redis.pubsubClient();
    // 通道有消息时触发
    client.on('message', exports.onMessage);
    // 订阅通道时触发
    client.on('subscribe', function() {
        exports.subscribed = true;
        while (exports.callbackQueue.length) {
            process.nextTick(exports.callbackQueue.shift());
        }
    });
    client.subscribe(client.getKey(exports.key)); // key events;
    exports.queue = require('../kue').singleton;
    //  exports.subscribed = true;
    exports.subscribeStarted = true;
};


/**
 * 取消订阅
 * @return {[type]} [description]
 */
exports.unsubscribe = function() {
    var client = redis.pubsubClient();
    client.unsubscribe();
    client.removeAllListeners();
    exports.subscribeStarted = false;
};


/**
 * Message handler.
 * 消息处理
 * @api private
 */
exports.onMessage = function(channel, msg) {
    // TODO: only subscribe on {Queue,Job}#on()
    msg = JSON.parse(msg);

    // map to Job when in-process
    var job = exports.jobs[msg.id];
    if (job) {
        job.emit.apply(job, msg.args);
        if (['complete', 'failed'].indexOf(msg.event) !== -1) exports.remove(job);
    }
    // emit args on Queues
    msg.args[0] = 'job ' + msg.args[0];
    msg.args.splice(1, 0, msg.id);
    if (exports.queue) {
        exports.queue.emit.apply(exports.queue, msg.args);
    }
};


/**
 * Emit `event` for for job `id` with variable args.
 * 发布消息
 * @param {Number} id 任务ID
 * @param {String} event 事件名字，不是redis通道号
 * @param {Mixed} ...
 * @api private
 */
exports.emit = function(id, event) {
    var client = redis.client(),
        msg = JSON.stringify({
            id: id,
            event: event,
            args: [].slice.call(arguments, 1) // 其它参数，除ID
        });
    client.publish(client.getKey(exports.key), msg); // key events;
};

// Worker pool
//

var sys = require('sys')
  , events = require('events')
  ;

function getLogger(name, level) {
  var log;
  level = level || process.env.work_pool_log || "info";

  try {
    log = require('log4js')().getLogger(name);
    log.setLevel(level);
  } catch(e) {
    log = { "trace": (level === "trace") ? console.log : function() {}
          , "debug": (level === "trace" || level === "debug") ? console.log : function() {}
          , "info" : console.log
          , "warn" : console.log
          , "error": console.log
          , "fatal": console.log
          }
  }

  return log;
}

function Pool (work_func) {
  var self = this;
  events.EventEmitter.call(self);

  self.work = work_func;
  if(typeof self.work !== "undefined" && typeof self.work !== 'function')
    throw new Error("Must provide a function as the worker for the pool");

  self.queue = {run: {}, incoming: []};
  self.size = 10;
  self.timeout = 0;
  self.virgin = true;
  self.completions = {"error":0, "success":0};
  self.log = getLogger('work_pool.Pool');

  var greatest_job_id = 0;
  self.new_job_id = function() {
    greatest_job_id += 1;
    return greatest_job_id.toString();
  }

  self.on('done', function(task) {
    if(!(self.queue.run[task.id]))
      throw new Error("Done callback job ID is not known: " + task.id);
    delete self.queue.run[task.id];
    self.emit('update');
  })

  // Track how many successes and failures there will be.
  self.on('error'  , function() { self.completions.error   += 1 });
  self.on('success', function() { self.completions.success += 1 });

  var waiting_for_drain = false;
  self.on('update', function() {
    //self.log.debug('update queue:\n' + sys.inspect(self.queue)); // XXX This statement destroys the CPU and locks things up.
    var job_ids = Object.keys(self.queue.run);

    if(self.queue.incoming.length === 0) {
      self.log.debug('No work to do');
      self.log.debug('waiting='+sys.inspect(waiting_for_drain) + ' running='+job_ids.length);
      if(waiting_for_drain && job_ids.length === 0) {
        waiting_for_drain = false;
        self.emit('drain');
      }
    } else {
      self.log.debug('Work to do');
      waiting_for_drain = true;

      if(job_ids.length >= self.size) {
        self.log.debug('No room for a new task');
      } else {
        // There is room for a new task.
        var task = self.queue.incoming.shift();
        self.queue.run[task.id] = task;

        // Emit "task" with parameters for its callbacks.
        self.run_task(task);
      }
    }
  })

  self.task_callback = function(task, er) {
    if(task.timed_out) {
      if(er) self.log.debug('Ignoring timed-out ' + task.label + "\nWith error:\n" + er);
      else   self.log.debug('Ignoring timed-out ' + task.label);
      return;
    } else if(task.timeout_id) {
      clearTimeout(task.timeout_id);
    }

    if(!(task.id in self.queue.run)) {
      if(er) throw new Error('Failed '  + label + " ID not known: " + job_id + "\nWith error:\n" + er);
      else   throw new Error('Success ' + label + " ID not known: " + job_id);
    }

    // Normal task in the run pool calling back.
    try {
      if(er)
        self.emit('error', er, task);
      else
        self.emit('success', task);
    } finally {
      self.emit('done', task);
    }
  }

  self.timed_out = function(task) {
    if(task.timed_out)
      throw new Error(task.label + " already timed out");

    var timeout_er = new Error("TIMEOUT: " + task.label);
    timeout_er.timeout = true;

    try {
      self.task_callback(task, timeout_er);
    } finally  {
      task.timed_out = true;
    }
  }

  self.run_task = function(task) {
    self.log.debug('Running task: ' + sys.inspect(task));

    if(!self.work)
      throw new Error("Work function is not defined");

    if(self.timeout)
      task.timeout_id = setTimeout(self.timed_out, self.timeout, task);

    try {
      self.work.apply(self, task.params);
    } catch (er) {
      self.emit('error', er, task);
    }
  }

}
sys.inherits(Pool, events.EventEmitter);
exports.Pool = Pool;

// Push parameters for a worker.
Pool.prototype.add = function() {
  var self = this;
  var job_id = self.new_job_id();
  var params = Array.prototype.slice.apply(arguments);
  var label = "Job " + job_id + ' ' + JSON.stringify(params);

  if(!self.work)
    throw new Error("Work function is not defined");

  if(self.virgin && (! self.timeout))
    self.log.warn("Adding tasks with no timeout. A callback bug in your code could make the pool fill and then freeze");
  self.virgin = false;

  var arity_without_callback = self.work.length - 1;
  if(params.length !== arity_without_callback)
    throw new Error(label + " requires " + arity_without_callback + " parameters");

  var task = {id:job_id, label:label, timed_out:false};
  task.params = params.concat(function(er) { return self.task_callback(task, er) });

  self.queue.incoming.push(task);
  self.emit('update');
}

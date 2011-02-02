// Worker pool
//

var sys = require('sys')
  , events = require('events')
  , LOG
  ;

try {
  LOG = require('log4js')().getLogger('work_pool');
  LOG.setLevel(process.env.work_pool_log || "info");
} catch(e) {
  LOG = { "debug": function() {}
        , "info" : console.log
        , "warn" : console.log
        , "error": console.log
        , "fatal": console.log
        }
}

function Pool (work_func) {
  var self = this;
  events.EventEmitter.call(self);

  if(typeof work_func !== 'function')
    throw new Error("Required function to perform the work");

  self.work = work_func;
  self.queue = {run: {}, incoming: []};
  self.size = 10;
  self.timeout = 0;

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

  var waiting_for_drain = false;
  self.on('update', function() {
    //LOG.debug('update queue:\n' + sys.inspect(self.queue)); // XXX This statement destroys the CPU and locks things up.
    var job_ids = Object.keys(self.queue.run);

    if(self.queue.incoming.length === 0) {
      LOG.debug('No work to do');
      LOG.debug('waiting='+sys.inspect(waiting_for_drain) + ' running='+job_ids.length);
      if(waiting_for_drain && job_ids.length === 0) {
        waiting_for_drain = false;
        self.emit('drain');
      }
    } else {
      LOG.debug('Work to do');
      waiting_for_drain = true;

      if(job_ids.length >= self.size) {
        LOG.debug('No room for a new task');
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
      if(er) LOG.debug('Ignoring timed-out ' + task.label + "\nWith error:\n" + er);
      else   LOG.debug('Ignoring timed-out ' + task.label);
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

    try {
      self.task_callback(task, timeout_er);
    } finally  {
      task.timed_out = true;
    }
  }

  self.run_task = function(task) {
    LOG.debug('Running task: ' + sys.inspect(task));

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

  var arity_without_callback = self.work.length - 1;
  if(params.length !== arity_without_callback)
    throw new Error(label + " requires " + arity_without_callback + " parameters");

  var task = {id:job_id, label:label, timed_out:false};
  task.params = params.concat(function(er) { return self.task_callback(task, er) });

  self.queue.incoming.push(task);
  self.emit('update');
}

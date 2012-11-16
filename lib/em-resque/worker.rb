require 'resque'

# A non-forking version of Resque worker, which handles waiting with
# a non-blocking version of sleep. 
class EventMachine::Resque::Worker < Resque::Worker
  attr_accessor :tick_instead_of_sleep
  attr_accessor :job

  def initialize(*args)
    super(*args)
    self.tick_instead_of_sleep = false
  end

  # Overwrite system sleep with the non-blocking version
  def sleep(interval)
    EM::Synchrony.sleep interval
  end

  # @param [Resque::Job] job
  def work_once job, &block
    startup
    log "got: #{job.inspect}"
    job.worker = self
    run_hook :before_fork, job  # do we really need this?
    working_on job

    procline "Processing #{job.queue} since #{Time.now.to_i}"
    perform(job, &block)

    done_working
    unregister_worker
  end
  
  # Be sure we're never forking
  def fork(*args)
    nil
  end

  # Simpler startup
  def startup
    register_worker
    @cant_fork = true
    $stdout.sync = true
  end

  # Tell Redis we've processed a job.
  def processed!
    Resque::Stat << "processed"
    Resque::Stat << "processed:#{self}"
    Resque::Stat << "processed_#{job.queue}"
    self.job = nil
  end

  def working_on job
    self.job = job
    super(job)
  end

  def unregister_worker(exception = nil)
    # If we're still processing a job, make sure it gets logged as a
    # failure.

    self.job.fail(exception || ::Resque::DirtyExit.new) if self.job

    redis.srem(:workers, self)
    redis.del("worker:#{self}")
    redis.del("worker:#{self}:started")

    Resque::Stat.clear("processed:#{self}")
    Resque::Stat.clear("failed:#{self}")
  end

  # The string representation is the same as the id for this worker instance.
  # Can be used with Worker.find
  def to_s
    "#{super}:#{Fiber.current.object_id}"
  end
  alias_method :id, :to_s
end

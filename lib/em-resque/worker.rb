require 'resque'

# A non-forking version of Resque worker, which handles waiting with
# a non-blocking version of sleep. 
class EventMachine::Resque::Worker < Resque::Worker
  attr_accessor :tick_instead_of_sleep
  attr_accessor :job

  def free?
    job.nil?
  end

  # @param [Resque::Job] job
  def work_once job, &block
    log "got: #{job.inspect}"
    job.worker = self
    working_on job

    procline "Processing #{job.queue} since #{Time.now.to_i}"
    perform(job, &block)

    done_working
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
  end

  # Called when we are done working - clears our `working_on` state
  # and tells Redis we processed a job.
  def done_working
    super
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
    "#{super}:#{object_id}"
  end
  alias_method :id, :to_s
end

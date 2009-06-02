require 'thread'


# :nodoc: namespace
module Tem::Mr::Search
  
class MapReduceExecutor
  # Creates an executor for a Map-Reduce job.
  #
  # Arguments:
  #   root_job:: the Map-Reduce job (see Tem::Mr::Search::MapReduceJob)
  #   db:: the database to run Map-Reduce over
  #   tems:: sessions to the available TEMs
  #   root_tem:: the index of the TEM that has the root mapper and reducer
  #   planner_class:: (optional) replacement for the default planner strategy
  def initialize(root_job, db, tems, root_tem, planner_class = nil)
    planner_class ||= MapReducePlanner
    
    @db = db  # Writable only in main thread.
    @tems = tems  # Writable only in main thread.

    # Writable only in main thread.
    @planner = planner_class.new @job, db.length, tems.length, root_tem
    
    # Protected by @lock
    @tem_jobs = { :mapper => { root_tem => root_job },
                  :reducer => { root_tem => root_job },
                  :finalizer => { root_tem => root_job } }
    # Protected by @lock
    @outputs = {}
    
    # Thread-safe.
    @thread_queues = tems.map { |tem| Queue.new }
    @main_queue = Queue.new
    @lock = Mutex.new
  end

  # Executes the job.
  def execute
    # Spawn TEM threads.
    @tems.each_index { |i| Thread.new(i) { |i| executor_thread i } }
    
    until @planner.done?
      actions = @planner.next_actions!
      @lock.synchronize do
        actions.each { |action| @thread_queues[action[:with]] << action }
      end
      
      action = @main_queue.pop
      @planner.action_done action
    end
    
    return @outputs[@planner.output_id]
  end
  
  # Main method for thread in charge of a TEM.
  def executor_thread(tem_index)
    queue = @thread_queues[tem_index]    
    while action = queue.pop
      execute_action action, tem_index
      @main_queue << action
    end
  end
  
  # Executes a Map-Reduce planner action.
  #
  # This method is called on the thread corresponding to the TEM that the action
  # is supposed to execute on.
  def execute_action(action, tem_index)
    case action[:action]
    when :migrate
      in_job = @lock.synchronize { @tem_jobs[action[:secpack]][tem_index] }
      out_job = in_job  # TODO(costan): actual migration
      @lock.synchronize do
        @tem_jobs[action[:secpack]][action[:to]] = out_job
      end
      
    when :map
      job, item = nil, nil
      @lock.synchronize do
        job = @tem_jobs[:mapper][tem_index]
        item = @db.item(action[:item])
      end
      output = job.map_object item, @tems[tem_index]
      @lock.synchronize do
        @outputs[action[:output_id]] = output
      end
      
    when :reduce
      job, output1, output2 = nil, nil, nil
      @lock.synchronize do
        job = @tem_jobs[:reducer][tem_index]
        output1 = @outputs[action[:output1_id]]
        output2 = @outputs[action[:output2_id]]
      end
      output = job.reduce_outputs output1, output2, @tems[tem_index]
      @lock.synchronize do
        @outputs[action[:output_id]] = output
      end

    when :finalize
      @lock.synchronize do
        job = @tem_jobs[:finalizer][tem_index]
        output = @outputs[action[:output_id]]
      end
      final_output = job.finalize_output output, @tems[tem_index]
      @lock.synchronize do
        @outputs[action[:final_id]] = final_output
      end
    end    
  end
end

end  # namespace Tem::Mr::Search

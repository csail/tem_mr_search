require 'thread'


# :nodoc: namespace
module Tem::Mr::Search
  
class MapReduceExecutor
  # Creates an executor for a Map-Reduce job.
  #
  # Arguments:
  #   job:: the Map-Reduce job (see Tem::Mr::Search::MapReduceJob)
  #   db:: the database to run Map-Reduce over
  #   tems:: sessions to the available TEMs
  #   root_tem:: the index of the TEM that has the root mapper and reducer
  #   planner_class:: (optional) replacement for the default planner strategy
  def initialize(job, db, tems, root_tem, planner_class = nil)
    planner_class ||= MapReducePlanner
    
    @db = db  # Writable only in main thread.
    @tems = tems  # Writable only in main thread.

    # Writable only in main thread.
    @planner = planner_class.new @job, db.length, tems.length, root_tem
    
    # Protected by @lock
    @tem_parts = { :mapper => { root_tem => job.mapper },
                   :reducer => { root_tem => job.reducer },
                   :finalizer => { root_tem => job.finalizer } }
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
      in_part = @lock.synchronize { @tem_parts[action[:secpack]][tem_index] }
      out_part = in_part  # TODO(costan): actual migration
      @lock.synchronize do
        @tem_parts[action[:secpack]][action[:to]] = out_part
      end
      
    when :map
      mapper, item = nil, nil
      @lock.synchronize do
        mapper = @tem_parts[:mapper][tem_index]
        item = @db.item(action[:item])
      end
      output = mapper.map_object item, @tems[tem_index]
      @lock.synchronize do
        @outputs[action[:output_id]] = output
      end
      
    when :reduce
      reducer, output1, output2 = nil, nil, nil
      @lock.synchronize do
        reducer = @tem_parts[:reducer][tem_index]
        output1 = @outputs[action[:output1_id]]
        output2 = @outputs[action[:output2_id]]
      end
      output = reducer.reduce_outputs output1, output2, @tems[tem_index]
      @lock.synchronize do
        @outputs[action[:output_id]] = output
      end

    when :finalize
      finalizer = nil
      @lock.synchronize do
        finalizer = @tem_parts[:finalizer][tem_index]
        output = @outputs[action[:output_id]]
      end
      final_output = finalizer.finalize_output output, @tems[tem_index]
      @lock.synchronize do
        @outputs[action[:final_id]] = final_output
      end
    end    
  end
end

end  # namespace Tem::Mr::Search

require 'rbtree'
require 'set'

# :nodoc: namespace
module Tem::Mr::Search
  
class MapReducePlanner  
  # Creates a planner for a Map-Reduce job.
  #
  # Arguments:
  #   job:: the Map-Reduce job (see Tem::Mr::Search::MapReduceJob)
  #   num_items: how many data items does the Map-Reduce run over
  #   num_tems:: how many TEMs are available
  #   first_tem:: the index of the TEM that has the jobs initially
  def initialize(job, num_items, num_tems, first_tem)
    @job = job
    @first_tem = first_tem
    
    @without = { :mapper => RBTree.new, :reducer => RBTree.new }
    @with = { :mapper => Set.new([first_tem]),
              :reducer => Set.new([first_tem]) }
    @free_tems = RBTree.new
    0.upto(num_tems - 1) do |tem|
      @free_tems[tem] = true
      next if tem == first_tem
      @without.each { |k, v| v[tem] = true }
    end
    
    @unmapped_items = (0...num_items).to_a.reverse
    @reduce_queue = RBTree.new
    @last_output_id = 0
    @last_reduce_id = 2 * num_items - 2
    @done_reducing, @output_id = false, nil
  end

  # Generates migrating actions for a SECpack type that are possible now.
  def migrate_actions(sec_type)
    actions = []
    return actions if @without[sec_type].length == 0
    free_tems = free_tems_with_sec sec_type
    free_tems.each do |source_tem|
      break if @without[sec_type].length == 0
      target_tem = @without[sec_type].min.first
      @without[sec_type].delete target_tem
      @free_tems.delete source_tem
      actions.push :action => :migrate, :secpack => sec_type,
                   :with => source_tem, :to => target_tem
    end
    actions
  end
  private :migrate_actions
  
  # Informs the planner that a SECpack migration has completed.
  def done_migrating(action)
    @free_tems[action[:with]] = true
    @with[action[:secpack]] << action[:to]
  end
  private :done_migrating
  
  # A sorted array of the free TEMs that have a SECpack type.
  def free_tems_with_sec(sec_type)
    tems = []
    @free_tems.each do |tem, true_value|
      tems << tem if @with[sec_type].include? tem
    end
    tems
  end
  
  # A unique output_id.
  def next_output_id
    next_id = @last_output_id
    @last_output_id += 1
    next_id
  end
  
  # Generates mapping actions possible right now.
  def map_actions
    actions = []
    return actions if @unmapped_items.empty?
    free_tems_with_sec(:mapper).each do |tem|
      break unless item = @unmapped_items.pop
      @free_tems.delete tem
      actions.push :action => :map, :item => item, :with => tem,
                   :output_id => next_output_id
    end
    actions
  end
  private :map_actions
  
  # Informs the planner that a data mapping has completed.
  def done_mapping(action)
    @free_tems[action[:with]] = true
    @reduce_queue[action[:output_id]] = true
  end
  private :done_mapping
  
  # Generates reducing actions possible right now.
  def reduce_actions
    actions = []
    return actions if @reduce_queue.length <= 1
    free_tems_with_sec(:reducer).each do |tem|
      break if @reduce_queue.length <= 1
      output1_id, output2_id = *[0, 1].map do |i|
        output_id = @reduce_queue.min.first
        @reduce_queue.delete output_id
        output_id
      end
      @free_tems.delete tem
      actions.push :action => :reduce, :with => tem, :output1_id => output1_id,
                   :output2_id => output2_id, :output_id => next_output_id
    end
    actions
  end
  private :reduce_actions
  
  # Informs the planner that a data reduction has completed.
  def done_reducing(action)
    @free_tems[action[:with]] = true
    if action[:output_id] == @last_reduce_id
      @done_reducing = true      
      return
    end
    @reduce_queue[action[:output_id]] = true
  end
  private :done_reducing
  
  # Generates finalizing actions possible right now.
  def finalize_actions
    return [] unless @done_reducing and !@output_id and @free_tems[@first_tem]
    @finalize_ready = false
    return [ :action => :finalize, :with => @first_tem,
             :output_id => @last_reduce_id, :final_id => next_output_id ]
  end
  private :finalize_actions
  
  # Informs the planner that an action issued by next_action was done.
  def done_finalizing(action)
    @free_tems[action[:with]] = true
    @output_id = action[:final_id]    
  end
  private :done_finalizing

  # True when the Map-Reduce job is complete.
  def done?
    !@output_id.nil?
  end
  
  # The output ID of the Map-Reduce's final result.
  attr_reader :output_id

  # Informs the planner that an action issued by next_actions was completed.
  def action_done(action)
    dispatch = { :migrate => :done_migrating, :map => :done_mapping, :reduce =>
                 :done_reducing, :finalize => :done_finalizing } 
    self.send dispatch[action[:action]], action
  end

  # Issues a set of actions that can be performed right now.
  def next_actions!
    actions = migrate_actions :mapper
    actions += migrate_actions :reducer
    actions += map_actions
    actions += reduce_actions
    actions += finalize_actions
    actions
  end  
end

end  # namespace Tem::Mr::search

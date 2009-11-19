# Allocates the individual components of a Map-Reduce job across TEMs.
#
# Author:: Victor Costan
# Copyright:: Copyright (C) 2009 Massachusetts Institute of Technology
# License:: MIT

require 'rbtree'
require 'set'

# :nodoc: namespace
module Tem::Mr::Search
  
# Allocates the individual components of a Map-Reduce job across TEMs.
#
# This class is instantiated and used by MapReduceExecutor. It should not be
# used directly in client code, except for the purpose of replacing the default
# planner.
#
# The Map-Reduce coordinator calls next_actions! on the planner, to obtain a
# list of actions that can be carried out. The planner guarantees that the
# actions are independent of each other, and that all their dependencies are
# satisfied. When the coordinator learns about the completion of some actions,
# it updates the planner's state by calling action_done. After action_done is
# called, new_action should be called again to obtain new actions that can be
# carried out.
#
# Partial results (outputs) in the Map-Reduce computation are identified by 
# unique numbers starting from 0. The output IDs can be used as file names, if
# the outputs are stored in a distributed file system. When the computation is
# done (calling done? returns +true+), the +output_id+ attribute will contain
# the ID of the computation's final result.
class MapReducePlanner  
  # Creates a planner for a Map-Reduce job.
  #
  # Arguments:
  #   job:: the Map-Reduce job (see Tem::Mr::Search::MapReduceJob)
  #   num_items: how many data items does the Map-Reduce run over
  #   num_tems:: how many TEMs are available
  #   root_tems:: the indexes of the TEMs that have the initial SECpacks bound
  #               to them (hash with the keys +:mapper+, +:reducer+ and
  #               +:finalizer+)
  def initialize(job, num_items, num_tems, root_tems)
    @job = job
    @root_tems = root_tems
    
    @without = { :mapper => RBTree.new, :reducer => RBTree.new }
    @with = { :mapper => Set.new([@root_tems[:mapper]]),
              :reducer => Set.new([@root_tems[:reducer]]) }
    @free_tems = RBTree.new
    
    # TEM ordering: the mapper root is first, the reducer root is last, and the
    #               finalizer root is second
    @ordered_tems = (0...num_tems).to_a
    @ordered_tems -= @root_tems.values
    @ordered_tems = [@root_tems[:mapper]] + @ordered_tems
    unless @ordered_tems.include? @root_tems[:reducer]
      @ordered_tems += [@root_tems[:reducer]]
    end
    unless @ordered_tems.include? @root_tems[:finalizer]
      @ordered_tems = [@ordered_tems[0], @root_tems[:finalizer]] +
                       @ordered_tems[1..-1]
    end
    # Reverted index for the TEM ordering.
    @rindex_tems = Array.new(num_tems)
    @ordered_tems.each_with_index { |t, i| @rindex_tems[t] = i }
    
    @ordered_tems.each_with_index do |tem, i|
      @free_tems[[i, tem]] = true
      @without.each { |k, v| v[[i, tem]] = true unless tem == @root_tems[k] }
    end
    
    @unmapped_items = (0...num_items).to_a.reverse
    @reduce_queue = RBTree.new
    @last_output_id = 0
    @last_reduce_id = 2 * num_items - 2
    @done_reducing, @output_id = false, nil
  end
  
  # Issues a set of actions that can be performed right now.
  #
  # The method alters the planner's state assuming the actions will be
  # performed.
  #
  # Returns an array of hashes, with one hash per action to be performed. The
  # +:action+ key specifies the type of action to be performed, and can be
  # +:migrate+ +:map+, +:reduce+, or +:finalize. All the actions have the
  # +:with+ key, which is the ID (0-based index) of the TEM that will be doing
  # the action.
  #
  # Migrate actions have the following keys:
  #   :secpack:: the type of SECpack to be migrated ( +:mapper+ or +:reducer+ )
  #   :with:: the ID of the TEM doing the migration  
  #   :to:: the number of the TEM that the SECpack should be migrated to
  #
  # Map actions have the following keys:
  #   :item_id:: the ID of the item to be mapped (number in Table-Scan order)
  #   :with:: the ID of the TEM doing the mapping
  #   :output_id:: ID for the result of the map operation
  #
  # Reduce actions have the following keys:
  #   :output1_id, :output2_id:: the IDs of the partial outputs to be reduced
  #   :with:: the ID of the TEM doing the reducing
  #   :output_id:: the ID for the result of the reduce operation
  #
  # The finalize action has the following keys:
  #   :output_id:: the ID of the last partial output, which will be finalized
  #   :with:: the ID of the TEM doing the finalization
  #   :final_id:: the ID for the computation's final result
  def next_actions!
    actions = migrate_actions :mapper
    actions += migrate_actions :reducer
    actions += reduce_actions
    actions += map_actions
    actions += finalize_actions
    actions
  end
  
  # Informs the planner that an action issued by next_actions! was completed.
  #
  # Args:
  #   action:: an action hash, as returned by next_actions!
  #
  # The return value is not specified.
  def action_done(action)
    dispatch = { :migrate => :done_migrating, :map => :done_mapping, :reduce =>
                 :done_reducing, :finalize => :done_finalizing } 
    self.send dispatch[action[:action]], action
  end

  # True when the Map-Reduce computation is complete.
  def done?
    !@output_id.nil?
  end
  
  # The output ID of the Map-Reduce's final result.
  attr_reader :output_id

  # Generates migrating actions for a SECpack type that are possible now.
  #
  # See next_actions! for a description of the return value.
  def migrate_actions(sec_type)
    actions = []
    return actions if @without[sec_type].length == 0
    free_tems = free_tems_with_sec sec_type
    free_tems.each do |source_tem|
      break if @without[sec_type].length == 0      
      target_tem = (sec_type == :mapper ? @without[sec_type].min :
                                          @without[sec_type].max).first.last
      @without[sec_type].delete [@rindex_tems[target_tem], target_tem]
      @free_tems.delete [@rindex_tems[source_tem], source_tem]
      actions.push :action => :migrate, :secpack => sec_type,
                   :with => source_tem, :to => target_tem
    end
    actions
  end
  private :migrate_actions
  
  # Informs the planner that a SECpack migration has completed.
  def done_migrating(action)
    @free_tems[[@rindex_tems[action[:with]], action[:with]]] = true
    @with[action[:secpack]] << action[:to]
  end
  private :done_migrating
    
  # Generates mapping actions possible right now.
  #
  # See next_actions! for a description of the return value.
  def map_actions
    actions = []
    return actions if @unmapped_items.empty?
    free_tems_with_sec(:mapper).each do |tem|
      break unless item = @unmapped_items.pop
      @free_tems.delete [@rindex_tems[tem], tem]
      actions.push :action => :map, :item => item, :with => tem,
                   :output_id => next_output_id
    end
    actions
  end
  private :map_actions
  
  # Informs the planner that a data mapping has completed.
  #
  # Args:
  #   action:: an action hash, as returned by map_actions
  #
  # The return value is not specified.
  def done_mapping(action)
    @free_tems[[@rindex_tems[action[:with]], action[:with]]] = true
    @reduce_queue[action[:output_id]] = true
  end
  private :done_mapping
  
  # Generates reducing actions possible right now.
  #
  # See next_actions! for a description of the return value.
  def reduce_actions
    actions = []
    return actions if @reduce_queue.length <= 1
    free_tems_with_sec(:reducer).reverse.each do |tem|
      break if @reduce_queue.length <= 1
      output1_id, output2_id = *[0, 1].map do |i|
        output_id = @reduce_queue.min.first
        @reduce_queue.delete output_id
        output_id
      end
      @free_tems.delete [@rindex_tems[tem], tem]
      actions.push :action => :reduce, :with => tem, :output1_id => output1_id,
                   :output2_id => output2_id, :output_id => next_output_id
    end
    actions
  end
  private :reduce_actions
  
  # Informs the planner that a data reduction has completed.
  #
  # Args:
  #   action:: an action hash, as returned by reduce_actions
  #
  # The return value is not specified.
  def done_reducing(action)
    @free_tems[[@rindex_tems[action[:with]], action[:with]]] = true
    if action[:output_id] == @last_reduce_id
      @done_reducing = true      
      return
    end
    @reduce_queue[action[:output_id]] = true
  end
  private :done_reducing
  
  # Generates finalizing actions possible right now.
  #
  # See next_actions! for a description of the return value.
  def finalize_actions
    root_tem = @root_tems[:finalizer]
    unless @done_reducing and !@output_id and
           @free_tems[[@rindex_tems[root_tem], root_tem]]
      return []
    end
    @finalize_ready = false
    return [ :action => :finalize, :with => root_tem,
             :output_id => @last_reduce_id, :final_id => next_output_id ]
  end
  private :finalize_actions
  
  # Informs the planner that an action issued by next_action was done.
  #
  # Args:
  #   action:: an action hash, as returned by finalize_actions
  #
  # The return value is not specified.
  def done_finalizing(action)
    @free_tems[[@rindex_tems[action[:with]], action[:with]]] = true
    @output_id = action[:final_id]    
  end
  private :done_finalizing

  # A sorted array of the free TEMs that have a SECpack type migrated to them.
  #
  # Args:
  #   sec_type:: the SECpack type (+:mapper+ or +:reducer+)
  def free_tems_with_sec(sec_type)
    tems = []
    @free_tems.each do |index_tem, true_value|
      tems << index_tem.last if @with[sec_type].include? index_tem.last
    end
    tems
  end
  private :free_tems_with_sec
  
  # Generates a unique output ID.
  #
  # Returns the unique output ID, which is a non-negative integer. Future calls
  # of this method are guaranteed to return different output IDs.
  def next_output_id
    next_id = @last_output_id
    @last_output_id += 1
    next_id
  end
  private :next_output_id
end  # class Tem::Mr::Search::MapReducePlanner

end  # namespace Tem::Mr::Search

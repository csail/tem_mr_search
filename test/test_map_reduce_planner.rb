require 'test/mr_test_case'
require 'yaml'

class MapReducePlannerTest < Test::Unit::TestCase
  MRPlanner = Tem::Mr::Search::MapReducePlanner
  
  def setup
    @testdata_path = File.join(File.dirname(__FILE__), '..', 'testdata')
  end
  
  def parallel_planning(planner)
    all_actions = []
    until planner.done?
      actions = planner.next_actions!
      all_actions << actions
      actions.each { |action| planner.action_done action }
    end
    all_actions    
  end
  
  def serial_planning(planner)    
    all_actions = []
    pending_actions = []
    until planner.done?
      actions = planner.next_actions!
      all_actions << actions
      pending_actions += actions
      action = pending_actions.shift   
      planner.action_done action if action 
    end
    all_actions
  end
  
  def _test_planning(method_name, items, tems, root_tem, gold_file)
    planner = MRPlanner.new nil, items, tems, root_tem
    all_actions = self.send method_name, planner
    gold_actions = File.open(File.join(@testdata_path, gold_file), 'r') do |f|
      YAML.load f
    end
    assert_equal gold_actions, all_actions, "Failed #{method_name}: " +
        "#{tems} tems with root #{root_tem}, #{items} items"
    assert_equal items * 2 - 1, planner.output_id, "Wrong final output_id"
  end
  
  def test_planning
    [[:parallel_planning, 7, 4, 0, 'parallel_plan_740.yml'],
     [:parallel_planning, 4, 3, 1, 'parallel_plan_431.yml'],
     [:serial_planning, 4, 1, 0, 'serial_plan_410.yml'],
     [:serial_planning, 7, 4, 0, 'serial_plan_740.yml'],
     [:serial_planning, 4, 3, 1, 'serial_plan_431.yml'],
    ].each do |testcase|
      _test_planning *testcase
    end
  end  
end

require 'test/mr_test_case'

class DbTest < MrTestCase  
  def test_loading    
    assert_equal 8, @db.length, 'Number of items in the database'
    gold_item = {"price" => 2500, "from" => "BOS", "to" => "TPE",
                 "flight" => 15, "layovers"=>2, "end_time"=>2100,
                 "start_time"=>900}
    assert_equal gold_item, @db.item(0), 'First database item'
  end
  
  def test_by_id
    assert_equal 18, @db.item_by_id(18)['flight'], 'Finding existing item by ID'
    assert_equal nil, @db.item_by_id(5), 'Finding non-existing item'
  end
end

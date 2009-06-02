require 'test/mr_test_case'

class DbTest < MrTestCase  
  def test_loading    
    assert_equal 4, @db.length, 'Number of items in the database'
    gold_item = {'from' => 'BOS', 'to' => 'NYC', 'price' => 150, 'start' => 900,
                 'end' => 1000, 'flight' => 15 }
    assert_equal gold_item, @db.item(0), 'First database item'
  end
end

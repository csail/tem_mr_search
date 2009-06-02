require 'test/mr_test_case'

class QueryBuilderTest < MrTestCase
  def setup
    super
    Tem.auto_conf
    $tem.activate
    $tem.emit
  end
  
  def _test_map_fare(fare)
    enc_output = @client_query.map_object fare_id(fare), fare, $tem
    output = @client_query.unpack_output enc_output
    assert_equal fare_id(fare), output[:id], 'Object ID incorrectly encoded'
    assert_equal fare_score(fare), output[:score],
                 'Score incorrectly computed'
    enc_output
  end
  
  def test_map_reduce
    fare1 = @db.item 0
    output1 = _test_map_fare fare1
    fare2 = @db.item 1
    output2 = _test_map_fare fare2
    
    win_fare = (fare_score(fare1) > fare_score(fare2)) ? fare1 : fare2
    # Try both permutations to ensure all branches of the reduce code work.
    [[output1, output2], [output2, output1]].each do |o1, o2|
      enc_output = @client_query.reduce_outputs o1, o2, $tem
      output = @client_query.unpack_output enc_output
      assert_equal fare_id(win_fare), output[:id], 'The wrong fare won (bad ID)'
      assert_equal fare_score(win_fare), output[:score],
                   'The wrong fare won (bad score)'
      assert_equal [1, 2, 3], output[:check], 'Incorrect check bytes'

      assert_not_equal enc_output, output1, 'Nonce fail'
      assert_not_equal enc_output, output2, 'Nonce fail'
    end
  end
end

require 'test/mr_test_case'

class QueryTest < MrTestCase
  def setup
    super
    
    @obj1 = @db.data[0]
    @obj2 = @db.data[1]
    @output1 = (1..16).to_a
    @output2 = (17..32).to_a
  end
  
  def test_map_for_object
    secpack = @client_query.map_for_object 0x12345678, @obj1
    
    assert_equal [0, 0, 0, 0, 0x12, 0x34, 0x56, 0x78],
                 secpack.get_bytes(:_id, 8), 'Object ID embedded incorrectly'
    assert_equal @obj1['price'], secpack.get_value(:price, :tem_short),
                 'Price embedded incorrectly'
    assert_equal @obj1['start'], secpack.get_value(:start, :tem_short),
                 'Starting time embedded incorrectly'
    assert_equal @obj1['end'], secpack.get_value(:end, :tem_short),
                 'Ending time embedded incorrectly'
  end
  
  def test_reduce_for_outputs
    secpack = @client_query.reduce_for_outputs @output1, @output2
    
    assert_equal @output1, secpack.get_bytes(:_output1, 16),
                 'Output1 embedded incorrectly'
    assert_equal @output2, secpack.get_bytes(:_output2, 16),
                 'Output2 embedded incorrectly'
  end
  
  def test_unpack_unencrypted_output
    packed_output = [0, 0, 0, 0, 0x12, 0x34, 0x56, 0x78, 0x31, 0x41, 0xCC, 0xCD,
                     0xCE, 0xBE, 0xEF, 0xFE]
    output = @client_query.unpack_decrypted_output packed_output
              
    assert_equal 0x12345678, output[:id], 'ID incorrectly unpacked'
    assert_equal 0x3141, output[:score], 'Score incorrectly unpacked'
    assert_equal [0xBE, 0xEF, 0xFE], output[:check], 'Check bytes'    
  end
end

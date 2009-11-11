require 'test/mr_test_case'

class MapReduceExecutorTest < MrTestCase
  MRExecutor = Tem::Mr::Search::MapReduceExecutor
  
  def setup
    super
    Tem.auto_conf
    $tem.emit if $tem.activate
    @thread_abort = Thread.abort_on_exception
    Thread.abort_on_exception = true
  end
  
  def teardown
    Thread.abort_on_exception = @thread_abort
    $tem.disconnect
    super
  end
      
  def _test_executor(tems, root_tem)
    executor = MRExecutor.new @client_query, @db, tems, root_tem
    packed_output = executor.execute
    result = @client_query.unpack_output packed_output
    gold_item = @db.item 5 
    assert_equal fare_id(gold_item), result[:id],
                 'Incorrect Map-Reduce result (ID)'
    assert_equal fare_score(gold_item), result[:score],
                 'Incorrect Map-Reduce result (score)'
  end
  
  def test_executor_with_autoconf
    _test_executor [$tem], 0
  end
  
  def test_executor_with_cluster    
    tems = Tem::Mr::Search::Server.tems_from_cluster_file @cluster_file
    assert_equal 8, tems.length, 'Incorrect cluster setup'
    
    tems.each { |tem| tem.emit if tem.activate }
    
    _test_executor tems, 0
  end
end

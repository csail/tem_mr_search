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
      
  def _test_executor(tems, root_tems)
    certs = {}
    [:mapper, :reducer, :finalizer].each do |sec|
      certs[sec] = tems[root_tems[sec]].pubek
    end
    @client_query.bind certs
    executor = MRExecutor.new @client_query, @db, tems, root_tems
    data = executor.execute
    result = @client_query.unpack_output data[:result]
    
    gold_item = @db.item 5 
    assert_equal fare_id(gold_item), result[:id],
                 'Incorrect Map-Reduce result (ID)'
    assert_equal fare_score(gold_item), result[:score],
                 'Incorrect Map-Reduce result (score)'
  
    assert data[:timings], 'No timings returned'
    assert data[:timings][:tasks], 'No tasks data in the timings'
    [:tem_ids, :migrate, :map, :reduce, :finalize].each do |task|
      assert data[:timings][:tasks][task], "No data on #{task} in the timings"
    end
    assert_operator data[:timings][:tems], :kind_of?, Array,
                    'No per-TEM data in the timings'
    assert data[:timings][:total], 'No total time in the timings'

    # Dump timing stats to show scheduler performance.
    p data[:timings]
  end
  
  def test_executor_with_autoconf
    _test_executor [$tem], {:mapper => 0, :reducer => 0, :finalizer => 0}
  end
  
  def test_executor_with_cluster    
    tems = Tem::Mr::Search::Server.tems_from_cluster_file @cluster_file
    assert_equal 8, tems.length, 'Incorrect cluster setup'
    
    tems.each { |tem| tem.emit if tem.activate }
    
    _test_executor tems, {:mapper => 0, :reducer => 7, :finalizer => 0}
  end
end

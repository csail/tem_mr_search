require 'test/mr_test_case'

class MapReduceExecutorTest < MrTestCase
  MRExecutor = Tem::Mr::Search::MapReduceExecutor
  
  def setup
    super
    Tem.auto_conf
    $tem.activate
    $tem.emit
  end
    
  def _test_executor(tems, root_tem)
    executor = MRExecutor.new @client_query, @db, tems, root_tem
    packed_output = executor.execute
    result = @client_query.unpack_output packed_output
    assert_equal 18, result[:id], 'Incorrect Map-Reduce result'
  end
  
  def test_executor_with_autoconf
    _test_executor [$tem], 0
  end
  
  def test_executor_with_cluster
    cluster_config = ['lightbulb2.local', 'darkbulb.local'].map { |host|
      Tem::MultiProxy::Client.query_tems host    
    }.flatten
    assert_equal 8, cluster_config.length, 'Incorrect cluster setup'
    tems = cluster_config.map do |config|
      Tem::Session.new Tem::Transport::AutoConfigurator.try_transport(config)
    end
    
    tems.each { |tem| tem.activate; tem.emit }
    
    _test_executor tems, 0
  end
end

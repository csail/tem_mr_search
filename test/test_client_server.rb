require 'test/mr_test_case'
require 'flexmock/test_unit'


class ClientServerTest < MrTestCase
  Client = Tem::Mr::Search::Client
  Server = Tem::Mr::Search::Server
    
  def setup
    super
    @server_port = 29552
  end
    
  def _test_request
    Thread.new do
      Server.new(@db_path, @empty_cluster_file, @server_port).serve_loop
    end
    sleep 0.1
    yield "localhost:#{@server_port}"
    Client.shutdown_server "localhost:#{@server_port}"
  end
  
  def test_fetch_item
    _test_request do |server_addr|
      fetched_item = Client.fetch_item server_addr, fare_id(@db.item(3))
      assert_equal @db.item(3), fetched_item, 'Fetch fail' 
    end
  end
  
  def test_query
    Tem.auto_conf
    @server_port = 29553
    flexmock(Server).should_receive(:tems_from_cluster_file).
                     with(@empty_cluster_file).and_return [$tem]
    _test_request do |server_addr|
      result = Client.search server_addr, @client_query
      gold_item = @db.item 5
      assert_equal fare_id(gold_item), result[:id],
                   'Incorrect Map-Reduce result (ID)'
      assert_equal fare_score(gold_item), result[:score],
                   'Incorrect Map-Reduce result (score)'
    end
  end
end

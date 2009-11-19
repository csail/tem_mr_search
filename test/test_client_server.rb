require 'test/mr_test_case'
require 'flexmock/test_unit'

class ClientServerTest < MrTestCase
  Client = Tem::Mr::Search::Client
  Server = Tem::Mr::Search::Server
    
  def setup
    super
    @server_port = 29550
    @thread_abort = Thread.abort_on_exception
    Thread.abort_on_exception = true
  end
  
  def teardown
    Thread.abort_on_exception = @thread_abort
    super
  end
    
  def _test_request
    server = Server.new(@db_path, @empty_cluster_file, @server_port)
    server_thread = Thread.new do
      server.serve_loop
    end
    Kernel.sleep 0.1  # Wait for the server to start.
    yield "localhost:#{@server_port}"
    Client.shutdown_server "localhost:#{@server_port}"
    server_thread.join
    Kernel.sleep 0.1  # Wait for the server to cleanup after itself.
  end
  
  def test_fetch_item
    _test_request do |server_addr|
      fetched_item = Client.fetch_item server_addr, fare_id(@db.item(3))
      assert_equal @db.item(3), fetched_item, 'Fetch fail' 
    end
  end
  
  def test_get_tem
    flexmock(Server).should_receive(:tems_from_cluster_file).
                     with(@empty_cluster_file).and_return do |file|
      Tem.auto_conf
      $tem.emit if $tem.activate
      [$tem]
    end
    
    _test_request do |server_addr|
      tem_info = Client.get_tem server_addr
      assert_equal 0, tem_info[:id], 'Incorrect TEM id'
      assert_equal $tem.endorsement_cert.to_pem, tem_info[:ecert].to_pem,
                   'Incorrect ECert'
      assert_equal $tem.pubek.ssl_key.to_pem, tem_info[:pubek].ssl_key.to_pem,
                   'Incorrect PubEK'
    end
  end
  
  def test_dump_database
    _test_request do |server_addr|
      items = Client.dump_database server_addr
      assert_equal @db.length, items.length, 'Wrong number of items'
      items.each_with_index do |item, i|
        assert_equal @db.item(i), item, "Discrepancy in item #{i}"
      end
    end    
  end
  
  def test_query
    flexmock(Server).should_receive(:tems_from_cluster_file).
                     with(@empty_cluster_file).and_return do |file|
      Tem.auto_conf
      [$tem]
    end
    _test_request do |server_addr|
      data = Client.search server_addr, @client_query
      gold_item = @db.item 5
      assert_equal fare_id(gold_item), data[:result][:id],
                   'Incorrect Map-Reduce result (ID)'
      assert_equal fare_score(gold_item), data[:result][:score],
                   'Incorrect Map-Reduce result (score)'
      assert data[:timings], 'No timing statistics in Map-Reduce result'
    end
  end
end

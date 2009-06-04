require 'logger'
require 'yaml'

# :nodoc: namespace
module Tem::Mr::Search

class Server
  DEFAULT_PORT = 9052
  
  OP = Zerg::Support::Protocols::ObjectProtocol
  OPAdapter  = Zerg::Support::Sockets::ProtocolAdapter.adapter_module OP
  
  # Creates a new Map-Reduce server (master).
  def initialize(db_file, cluster_file, port)
    @logger = Logger.new STDERR
    @db = Db.new db_file
    @cluster_file = cluster_file
    @tems = []
    refresh_tems!
    @port = port || DEFAULT_PORT
  end
  
  # Reinitializes the TEM cluster connections.
  #
  # This should be called reasonably often to be able to respond to cluster
  # configuration changes.
  def refresh_tems!
    @tems.each { |tem| tem.disconnect }
    @tems = Server.tems_from_cluster_file @cluster_file
  end

  # This server's loop.
  def serve_loop
    listen_socket = Zerg::Support::SocketFactory.socket :in_port => @port
    listen_socket.listen
    shutdown_received = false
    until shutdown_received
      begin
        client_socket, client_addr = listen_socket.accept
        client_socket.extend OPAdapter
        request = client_socket.recv_object
        begin
          response = process_request request
        rescue Exception => e
          @logger.error e
          response = nil
        end        
        client_socket.send_object response if response
        shutdown_received = true if response == :shutdown
      rescue Exception => e
        @logger.error e
      end
      client_socket.close
    end
    listen_socket.close
  end
  
  # Computes the response of a single request.
  def process_request(request)    
    case request[:type]
    when :search
      refresh_tems!
      job = MapReduceJob.new request[:map_reduce]
      root_tem = request[:root_tem]
      executor = MapReduceExecutor.new job, @db, @tems, root_tem
      return executor.execute
    when :fetch
      return @db.item_by_id(request[:id]) || :not_found
    when :shutdown
      return :shutdown
    when :db_dump
      return (0...@db.length).map { |i| @db.item(i) }
    else
      return :unknown
    end
  end
  
  # Creates sessions to all the TEMs in a cluster.
  def self.tems_from_cluster_file(cluster_file)
    cluster_hosts = File.open(cluster_file, 'r') { |f| YAML.load f }
    cluster_configs = cluster_hosts.map { |host|
      Tem::MultiProxy::Client.query_tems host
    }.flatten
    cluster_configs.reject { |config| config.nil? }.map do |config|
      Tem::Session.new Tem::Transport::AutoConfigurator.try_transport(config)
    end
  end
end

end  # namespace Tem::Mr::Search

# :nodoc: namespace
module Tem::Mr::Search

class Client
  OP = Zerg::Support::Protocols::ObjectProtocol
  OPAdapter = Zerg::Support::Sockets::ProtocolAdapter.adapter_module OP
  
  # Performs a private database search using a Map-Reduce.
  def self.search(server_addr, client_query)
    output = issue_request server_addr, :type => :search, :root_tem => 0,
                                        :map_reduce => client_query.to_hash
    output ? client_query.unpack_output(output) : nil
  end
  
  # Asks for an item in the server's database.
  #
  # In production, there should be per-client rate-limiting on this request.
  def self.fetch_item(server_addr, item_id)
    issue_request server_addr, :type => :fetch, :id => item_id
  end
  
  # Terminates the server.
  #
  # In production, normal clients wouldn't have access to this.
  def self.shutdown_server(server_addr)
    issue_request server_addr, :type => :shutdown
  end
  
  # Dumps the server database.
  #
  # In production, normal clients wouldn't have access to this.
  def self.dump_database(server_addr)
    issue_request server_addr, :type => :db_dump
  end
  
  # Issues a request against a Map-Reduce server and returns the response.
  def self.issue_request(server_addr, request)
    socket = Zerg::Support::SocketFactory.socket :out_addr => server_addr,
        :out_port => Server::DEFAULT_PORT, :no_delay => true
    socket.extend OPAdapter
    socket.send_object request
    response = socket.recv_object response
    socket.close
    response
  end
end

end  # namespace Tem::Mr::Search

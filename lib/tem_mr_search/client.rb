# Client for the map-reduce RPC server.
#
# Author:: Victor Costan
# Copyright:: Copyright (C) 2009 Massachusetts Institute of Technology
# License:: MIT

# :nodoc: namespace
module Tem::Mr::Search


# Client for the map-reduce RPC server.
class Client
  OP = Zerg::Support::Protocols::ObjectProtocol
  OPAdapter = Zerg::Support::Sockets::ProtocolAdapter.adapter_module OP
  
  # Requests information for a random TEM to be used as a query's root TEM.
  #
  # Args:
  #   server_addr:: string with the address of the Map-Reduce server's RPC port.
  #
  # Returns a hash with the following keys:
  #   :id:: the TEM's ID (to be used as the :root_tem argument in search calls)
  #   :ecert:: the TEM's Endorsement Certificate
  #   :pubek:: the TEM's public Endorsement Key (from the ECert)
  def self.get_tem(server_addr)
    output = issue_request server_addr, :type => :get_tem
    return nil unless output
    
    ecert = OpenSSL::X509::Certificate.new output[:ecert]
    pubek = Tem::Key.new_from_ssl_key ecert.public_key
    { :id => output[:id], :ecert => ecert, :pubek => pubek }
  end
  
  # Performs a private database search using a Map-Reduce.
  #
  # Args:
  #   server_addr:: string with the address of the Map-Reduce server's RPC port.
  #   client_query:: a ClientQuery instance expressing the Map-Reduce search
  #
  # Returns a hash with the following keys:
  #   :result:: the result of the Map-Reduce computation
  #   :timings:: timing statistics on the job's execution 
  def self.search(server_addr, client_query)
    tem_certs = {}
    tem_ids = {}
    [:mapper, :reducer, :finalizer].each do |sec|
      tem_info = get_tem server_addr
      tem_ids[sec] = tem_info[:id]
      # TODO: check the endorsement certificate.
      tem_certs[sec] = tem_info[:pubek]
    end
    client_query.bind tem_certs
    
    output = issue_request server_addr, :type => :search,
                                        :root_tems => tem_ids,
                                        :map_reduce => client_query.to_hash
    return nil unless output
    output.merge! :result => client_query.unpack_output(output[:result])
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
  #
  # This method should not be called directly.
  def self.issue_request(server_addr, request)
    socket = Zerg::Support::SocketFactory.socket :out_addr => server_addr,
        :out_port => Server::DEFAULT_PORT, :no_delay => true
    socket.extend OPAdapter
    socket.send_object request
    response = socket.recv_object response
    socket.close
    response
  end
end  # class Tem::Mr::Search::Client

end  # namespace Tem::Mr::Search

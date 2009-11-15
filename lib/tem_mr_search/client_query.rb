# :nodoc: namespace
module Tem::Mr::Search

class ClientQuery < MapReduceJob
  def initialize(attributes)
    super
    @query_key = attributes[:key]
  end

  # Unpacks a reduce output into its components.
  #
  # This is expected to be called with the encrypted output returned by the
  # search provider.
  def unpack_output(output)
    decrypted_output = @query_key.decrypt output
    unpack_decrypted_output decrypted_output
  end
end
  
end  # namespace Tem::Mr::search

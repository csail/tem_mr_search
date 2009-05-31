# :nodoc: namespace
module Tem::Mr::Search

class ClientQuery < Query
  def initialize(attributes)
    super
    @query_key = attributes[:key]
  end

  def unpack_output(output)
    # TODO(costan): decrypt output once we enable encryption
    decrypted_output = output
    unpack_decrypted_output decrypted_output
  end
end
  
end  # namespace Tem::Mr::search

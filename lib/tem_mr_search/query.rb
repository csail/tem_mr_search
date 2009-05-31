# :nodoc: namespace
module Tem::Mr::Search
  
class Query  
  attr_reader :map_secpack, :reduce_secpack, :attributes
  
  def initialize(attributes)
    @map_secpack = attributes[:map]
    @reduce_secpack = attributes[:reduce]
    @attributes = attributes[:attributes]
  end
  
  # Returns a SECpack for mapping the given object data into the query.
  def map_for_object(object_id, object_data)
    secpack = Tem::SecPack.new_from_array map_secpack.to_array
    secpack.set_bytes :_id, [object_id].pack('q').unpack('C*').reverse
    attributes.each do |attribute|
      name, type = attribute[:name], attribute[:type]
      secpack.set_value name.to_sym, type, object_data[name.to_s]
    end
    secpack
  end
  
  # Maps the given object into the query.
  def map_object(object_id, object_data, tem)
    secpack = map_for_object object_id, object_data
    tem.execute secpack
  end

  # Returns a SECpack for reducing two inputs coming from maps or other reduces.
  def reduce_for_outputs(output1, output2)
    secpack = Tem::SecPack.new_from_array reduce_secpack.to_array
    
    secpack.set_bytes :_output1, output1
    secpack.set_bytes :_output2, output2
    secpack
  end
  
  # Reduces two inputs coming from maps or other reduces.
  def reduce_outputs(output1, output2, tem)
    secpack = reduce_for_outputs output1, output2
    tem.execute secpack
  end
  
  # Unpacks a decrypted output into its components.
  def unpack_decrypted_output(output)
    {
      :id => output[0, 8].reverse.pack('C*').unpack('q').first,
      :score => Tem::Abi.read_tem_short(output, 8),
      :check => output[13, 3]
    }
  end
end

end  # namespace Tem::Mr::search

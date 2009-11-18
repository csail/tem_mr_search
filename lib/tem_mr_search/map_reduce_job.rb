# Expresses a Map-Reduce computation whose components are performed on TEMs.
#
# Author:: Victor Costan
# Copyright:: Copyright (C) 2009 Massachusetts Institute of Technology
# License:: MIT

# :nodoc: namespace
module Tem::Mr::Search
  
# Expresses a Map-Reduce computation whose components are performed on TEMs.
#
# Client code should put together Map-Reduce computations using QueryBuilder or
# similar builder classes. In turn, builders construct and return MapReduceJob
# instances.
#
# Jobs can be serialized to a hash for network transmission. To de-serialize a
# job, pass the hash to the hash constructor:
#     hash = job.to_hash
#     job = MapReduceJob.new hash
class MapReduceJob  
  attr_reader :mapper, :reducer, :finalizer, :attributes, :id_attribute
  
  def initialize(attributes)
    @attributes = attributes[:attributes]
    @id_attribute = attributes[:id_attribute]

    @mapper = Mapper.new attributes[:map], self
    @reducer = Reducer.new attributes[:reduce], self
    @finalizer = Finalizer.new attributes[:finalize], self
  end
  
  # Unpacks a decrypted output into its components.
  def unpack_decrypted_output(output)
    {
      :id => output[0, 8].reverse.pack('C*').unpack('q').first,
      :score => Tem::Abi.read_tem_short(output, 8),
      :check => output[13, 10]
    }
  end
  
  # Serializes a job to a hash.
  #
  # Useful in conjunction with ObjectProtocol in ZergSupport, for sending jobs
  # across the wire. De-serialize with MapReduceJob#new
  def to_hash
    { :attributes => @attributes, :id_attribute => @id_attribute,
      :map => @mapper.to_plain_object, :reduce => @reducer.to_plain_object,
      :finalize => @finalizer.to_plain_object }
  end
  
  # Binds the SECpacks in this job to the given keys.
  #
  # Args:
  #   tem_pubeks:: the public keys to bind the SECpacks to; hash with the keys
  #                +:mapper+, +:reducer+ and +:finalizer+; each key is mapped to
  #                a TEM key
  def bind(tem_pubeks)
    @mapper.bind tem_pubeks[:mapper]
    @reducer.bind tem_pubeks[:reducer]
    @finalizer.bind tem_pubeks[:finalizer]
  end

  # Base class for the Map-Reduce SECpack wrappers.
  class JobPart
    def initialize(secpack, job)
      unless secpack.nil? or secpack.kind_of? Tem::SecPack
        secpack = Tem::SecPack.new_from_array secpack
      end
      @job = job
      @secpack = secpack
    end
    
    # Binds the wrapped SECpack to the given key.
    def bind(tem_pubek)
      @secpack.bind tem_pubek, :_secret, :_plain if @secpack
    end
    
    # Creates a copy of the job part by migrating the wrapped SECpack.
    #
    # Args:
    #   target_ecert:: the Endorsement Certificate of the target TEM
    #   tem:: session to the TEM to be used for migration
    def migrate(target_ecert, tem)
      migrated_secpack = tem.migrate @secpack, target_ecert
      self.class.new migrated_secpack, @job
    end
    
    def to_plain_object
      return nil if @secpack.nil?
      @secpack.to_array
    end
  end

  # Wrapper for the map SECpack.
  class Mapper < JobPart
    def initialize(secpack, job)
      super
      @attributes = job.attributes
      @id_attribute = job.id_attribute
    end
    
    # SECpack for mapping the given object data into the query.
    def map_for_object(object_data)
      return nil unless @secpack
      object_id = object_data[@id_attribute.to_s]    
      new_secpack = Tem::SecPack.new_from_array @secpack.to_array
      new_secpack.set_bytes :_id, [object_id].pack('q').unpack('C*').reverse
      @attributes.each do |attribute|
        name, type = attribute[:name], attribute[:type]
        new_secpack.set_value name.to_sym, type, object_data[name.to_s]
      end
      new_secpack
    end
    
    # Maps the given object into the query.
    def map_object(object_data, tem)    
      secpack = map_for_object object_data
      secpack ? tem.execute(secpack) : object_data
    end
  end  
  
  # Wrapper for the reduce SECpack.
  class Reducer < JobPart
    # SECpack for reducing two inputs coming from maps or other reduces.
    def reduce_for_outputs(output1, output2)
      new_secpack = Tem::SecPack.new_from_array @secpack.to_array
      
      new_secpack.set_bytes :_output1, output1
      new_secpack.set_bytes :_output2, output2
      new_secpack
    end

    # Reduces two inputs coming from maps or other reduces.
    def reduce_outputs(output1, output2, tem)
      secpack = reduce_for_outputs output1, output2
      tem.execute secpack
    end
  end
  
  # Wrapper for the finalize SECpack.
  class Finalizer < JobPart
    # Converts a map/reduce output into the final result for the operation.
    def finalize_output(output, tem)
      return output unless @secpack
      secpack = Tem::SecPack.new_from_array @finalize_secpack.to_array
      secpack.set_bytes :_output, output
      tem.execute secpack
    end
  end  
end

end  # namespace Tem::Mr::search

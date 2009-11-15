# :nodoc: namespace
module Tem::Mr::Search

class QueryBuilder
  # Build a Query.
  def self.query
    builder = self.new
    yield builder
    builder.query
  end
  
  # Defines the object attributes imported into the map method.
  def attributes(attributes)
    @attributes = attributes.to_a.map do |k, v|
      { :name => k,
        :type => v,
        :length => Tem::Abi.send(:"#{v}_length")        
      }
    end
  end
  
  # Defines the object attribute that's used as an object ID.
  def id_attribute(id_attribute)
    @id_attribute = id_attribute.to_sym
  end
  
  # Defines the query's map procedure.
  def map
    @map_secpack = Tem::Assembler.assemble do |s|
      s.label :_secret
      s.label :_key
      s.data :tem_ubyte, @query_key.to_tem_key
      s.label :_check_bytes
      s.data :tem_ubyte, @check_bytes
      
      # User-provided ranking procedure (secret).
      s.label :_ranking
      yield s
      s.ret
      
      s.entry
      s.ldbc 24
      s.outnew
      # Compute score.
      s.call :_ranking      
      # Compute padding.
      s.ldbc 3
      s.ldwc :_nonce
      s.rnd
      s.mcfxb :from => :_check_bytes, :to => :_check,
              :size => @check_bytes.length
      
      # Encrypt output.
      s.ldwc :const => :_key
      s.rdk
      s.kefxb :from => :_id, :size => 23, :to => 0xFFFF
      s.halt
      
      s.label :_plain
      
      # Make room for query attributes.
      @attributes.each do |attribute|
        s.label attribute[:name]
        s.zeros attribute[:type], 1
      end
      # Object ID.
      s.label :_id
      s.zeros :tem_ubyte, 8
      # Object score.
      s.label :score
      s.zeros :tem_short, 1
      # Random nonce to prevent matching map outputs.
      s.label :_nonce
      s.zeros :tem_ubyte, 3
      # Check bytes to prevent malicious input corruption.
      s.label :_check
      s.zeros :tem_ubyte, @check_bytes.length
      
      s.stack 64
    end    
  end
  
  # Defines the query's reduce procedure.
  def reduce
    @reduce_secpack = Tem::Assembler.assemble do |s|
      s.label :_secret
      s.label :_key
      s.data :tem_ubyte, @query_key.to_tem_key
      s.label :_check
      s.data :tem_ubyte, @check_bytes
      
      s.label :_signed
      # User-provided comparison procedure (signed).
      s.label :_comparison_proc
      yield s
      s.ret
      
      s.entry
      s.ldbc :const => 24
      s.outnew      
      # Decrypt inputs.
      s.ldwc :const => :_key
      s.rdk
      s.stw :_key_id
      [1, 2].each do |i|
        s.ldw :_key_id
        s.kdfxb :from => :"_output#{i}", :to => :"_id#{i}", :size => 24
        
        # Compare the check bytes and abort if the inputs were tampered with.
        s.mcmpfxb :op1 => :"_check#{i}", :op2 => :"_check",
                  :size => @check_bytes.length
        s.jz :"_check_#{i}_ok"
        s.halt
        s.label :"_check_#{i}_ok"
      end
      
      # Compare and output.
      s.call :_comparison_proc
      s.ldw :comparison
      s.jae :_output1_wins
      s.mcfxb :from => :_id2, :to => :_id1, :size => 16
      s.jmp :_output
      s.label :_output1_wins
      # Still do a memcpy, to prevent timing attacks.
      s.mcfxb :from => :_id2, :to => :_id2, :size => 16
      s.jmp :_output
      # Refresh the nonce to prevent learning about the comparison criteria.
      s.label :_output
      s.ldbc 3
      s.ldwc :_nonce1
      s.rnd
      # Encrypt output.
      s.ldwc :const => :_key
      s.rdk
      s.kefxb :from => :_id1, :size => 23, :to => 0xFFFF
      s.halt
      
      s.label :_plain
      # The comparison result produced by the user comparison procedure.
      s.label :comparison
      s.zeros :tem_short, 1
      s.label :_key_id
      s.zeros :tem_short, 1
      
      # The two inputs to reduce.
      [1, 2].each do |i|
        # Encrypted map/reduce output.
        s.label :"_output#{i}"        
        s.zeros :tem_ubyte, 24
        # Unencrypted input (decrypted inside TEM).
        s.label :"_id#{i}"
        s.zeros :tem_ubyte, 8
        s.label :"score#{i}"
        s.zeros :tem_short, 1
        s.label :"_nonce#{i}"
        s.zeros :tem_ubyte, 3        
        s.label :"_check#{i}"
        s.zeros :tem_ubyte, @check_bytes.length
      end
      s.stack 16
    end
  end
  
  def query
    raise "Map procedure not specified" unless @map_secpack
    raise "Reduce procedure not specified" unless @reduce_secpack
    raise "ID attribute not specified" unless @id_attribute
    
    ClientQuery.new :key => @query_key, :attributes => @attributes,
                    :map => @map_secpack, :reduce => @reduce_secpack,
                    :id_attribute => @id_attribute
  end

  def initialize
    @check_bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    @query_key = Tem::Keys::Symmetric.generate
  end
end  # class QueryBuilder

end  # namespace Tem::Mr::Search

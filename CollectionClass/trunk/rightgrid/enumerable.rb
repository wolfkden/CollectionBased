class ENUMERATION
  def ENUMERATION.add_item(key, value)
   @hash ||= {}
   @hash[key] = value
  end
  def ENUMERATION.const_missing(key)
    @hash[key]
  end
  def ENUMERATION.each
    @hash.each { |key, value| yield(key, value) }
  end
  def ENUMERATION.length
    @hash.length
  end

  ENUMERATION.add_item :VALUE,   0
  ENUMERATION.add_item :ORDINAL, 1
  ENUMERATION.add_item :FUNC,    2
end

class ENUM < ENUMERATION
  def ENUMERATION.add_item(key, value, ordinal = nil, func = nil)
   @hash ||= {}
   list = Array.new(ENUMERATION.length)
   list[ENUMERATION::VALUE] = value
   list[ENUMERATION::ORDINAL] = ordinal if ordinal
   list[ENUMERATION::FUNC] = func if func
   @hash[key] = list
  end
  def ENUM.const_missing(key)
    @hash[key][ENUMERATION::VALUE]
  end
  def ENUM.method_missing(symbol, args)
    case symbol
      when "ordinal" then ENUM.ordinal(args[0])
      when "each_ordinal" then ENUM.each_ordinal
      else "no ENUM  method found"
    end

  end
  def ENUM.each
    @hash.each { |key, value| yield(key, value[ENUMERATION::VALUE]) }
  end
  def ENUM.has_key?(key)
    return @hash.has_key?(key)
  end
  def ENUM.has_value?(value)
    hv = false
    self.each do |key, item| hv |= (item == value) end
    return hv
  end
        private
  def ENUM.ordinal(key)
    @hash[key][ENUMERATION::ORDINAL]
  end
        private
  def ENUM.each_ordinal
    @hash.each { |key, value| yield(key, value[ENUMERATION::ORDINAL]) }
  end
end

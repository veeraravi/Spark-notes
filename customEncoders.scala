def clazz[T](cls: Class[T], encoders: Seq[(String, ExpressionEncoder[_])]): ExpressionEncoder[T] = {
  encoders.foreach { case (_, enc) => enc.assertUnresolved() }

  val schema = StructType(encoders.map {
    case (fieldName, e) =>
      val (dataType, nullable) = if (e.flat) {
        e.schema.head.dataType -> e.schema.head.nullable
      } else {
        e.schema -> true
      }
      StructField(fieldName, dataType, nullable)
  }.toSeq)

  val serializer = encoders.map { case (fieldName, enc) =>
    val originalInputObject = enc.serializer.head.collect { case b: BoundReference => b }.head
    val newInputObject = Invoke(
      BoundReference(0, ObjectType(cls), nullable = true),
      fieldName,
      originalInputObject.dataType)

    val newSerializer = enc.serializer.map(_.transformUp {
      case b: BoundReference if b == originalInputObject => newInputObject
    })

    val serializerExpr = if (enc.flat) {
      newSerializer.head
    } else {
      val struct = CreateStruct(newSerializer)
      val nullCheck = Or(
        IsNull(newInputObject),
        Invoke(Literal.fromObject(None), "equals", BooleanType, newInputObject :: Nil))
      If(nullCheck, Literal.create(null, struct.dataType), struct)
    }
    Alias(serializerExpr, fieldName)()
  }

  val childrenDeserializers = encoders.zipWithIndex.map { case ((fieldName, enc), index) =>
    if (enc.flat) {
      enc.deserializer.transform {
        case g: GetColumnByOrdinal => g.copy(ordinal = index)
      }
    } else {
      val input = GetColumnByOrdinal(index, enc.schema)
      val deserialized = enc.deserializer.transformUp {
        case UnresolvedAttribute(nameParts) =>
          assert(nameParts.length == 1)
          UnresolvedExtractValue(input, Literal(nameParts.head))
        case GetColumnByOrdinal(ordinal, _) => GetStructField(input, ordinal)
      }
      If(IsNull(input), Literal.create(null, deserialized.dataType), deserialized)
    }
  }

  val deserializer =
    NewInstance(cls, childrenDeserializers, ObjectType(cls), propagateNull = false)

  new ExpressionEncoder[Any](
    schema,
    flat = false,
    serializer,
    deserializer,
    ClassTag(cls)).asInstanceOf[ExpressionEncoder[T]]

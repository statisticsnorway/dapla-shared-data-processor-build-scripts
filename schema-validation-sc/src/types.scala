package com.schemavalidation.types

import cats.syntax.all.*
import io.circe.*
import io.circe.derivation.*
import io.circe.generic.semiauto.*
import scala.collection.immutable.*

// Helper extensions for String type
extension (str: String)
  def red = str.split("\n").map(Console.RED + _).mkString("\n")
  def green = str.split("\n").map(Console.GREEN + _).mkString("\n")
  def newlines = s"\n\n$str\n\n"

enum PseudoOperation:
  case Pseudo, Depseudo, Repseudo // , Redact

given Decoder[PseudoOperation] = ConfiguredEnumDecoder.derive(_.toUpperCase)
given Encoder[PseudoOperation] = ConfiguredEnumEncoder.derive(_.toUpperCase)

enum EncryptionAlgorithm:
  case Default, PapisCompatible, SidMapping

given Decoder[EncryptionAlgorithm] = ConfiguredEnumDecoder.derived(using
  Configuration.default.withSnakeCaseConstructorNames
)

case class PseudoTask(
    name: String,
    columns: List[String],
    pseudoOperation: PseudoOperation,
    encryptionAlgorithm: EncryptionAlgorithm,
    encryptionArgs: Option[HashMap[String, String]]
)

given Decoder[HashMap[String, String]] with
  def apply(c: HCursor): Decoder.Result[HashMap[String, String]] =
    for listOfMaps <- c.as[List[Map[String, String]]]
    yield HashMap.from(listOfMaps.flatten)

given Decoder[PseudoTask] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)

case class DelomatenConfig(
    sharedBucket: String,
    sourceFolder: String,
    destinationFolder: String,
    memorySize: Int,
    pseudo: List[PseudoTask],
    outputColumns: Option[List[String]]
)
given Decoder[DelomatenConfig] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)

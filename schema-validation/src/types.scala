package com.schemavalidation.types

import cats.syntax.all.*
import io.circe.*
import io.circe.derivation.*
import io.circe.generic.semiauto.*
import scala.collection.immutable.*
import java.util.Date
import scala.util.Try
import java.text.SimpleDateFormat
import no.ssb.dapla.kuben.v1.{SharedBucket, SharedBuckets}
import scala.jdk.CollectionConverters._

enum PseudoOperation:
  case Pseudo, Depseudo // ,Repseudo, Redact

given Decoder[PseudoOperation] = ConfiguredEnumDecoder.derive(_.toUpperCase)
given Encoder[PseudoOperation] = ConfiguredEnumEncoder.derive(_.toUpperCase)

enum SidMapFailureStrategy:
  case ReturnNull, ReturnOriginal

val sidMapFailureStrategyConfig =
  Configuration.default.withTransformConstructorNames { constructor =>
    constructor.split("(?=[A-Z])").map(_.toUpperCase).mkString("_")
  }

given Decoder[SidMapFailureStrategy] =
  ConfiguredEnumDecoder.derived(using sidMapFailureStrategyConfig)
given Encoder[SidMapFailureStrategy] =
  ConfiguredEnumEncoder.derived(using sidMapFailureStrategyConfig)

enum EncryptionKey:
  case SsbCommonKey1, SsbCommonKey2, PapisCommonKey1

val encryptionKeyConfig =
  Configuration.default.withTransformConstructorNames { constructor =>
    constructor.split("(?=[A-Z0-9])").map(_.toLowerCase).mkString("-")
  }

given Decoder[EncryptionKey] =
  ConfiguredEnumDecoder.derived(using encryptionKeyConfig)
given Encoder[EncryptionKey] =
  ConfiguredEnumEncoder.derived(using encryptionKeyConfig)

enum EncryptionAlgorithm:
  case Default(encryptionKey: EncryptionKey)
  case PapisCompatible(encryptionKey: EncryptionKey)
  case SidMapping(
      encryptionKey: EncryptionKey,
      sidSnapshotDate: Option[Date],
      sidOnMapFailure: Option[SidMapFailureStrategy]
  )

given Decoder[Date] = Decoder.decodeString.emapTry { s =>
  val format = SimpleDateFormat("yyyy-MM-dd")
  Try(format.parse(s))
}

given Encoder[Date] = Encoder.instance { (date: Date) =>
  Json.fromString(SimpleDateFormat("yyyy-MM-dd").format(date))
}

given Decoder[EncryptionAlgorithm] = Decoder.instance { (c: HCursor) =>
  import EncryptionKey.*
  import EncryptionAlgorithm.*
  for
    algorithm <- c.downField("algorithm").as[String]
    key <- c
      .getOrElse("key")(
        if algorithm == "default" then SsbCommonKey1 else PapisCommonKey1
      )
    sidSnapshotDate <- c.downField("sid_snapshot_date").as[Option[Date]]
    sidOnMapFailure <- c
      .downField("sid_on_map_failure")
      .as[Option[SidMapFailureStrategy]]
    decode <- (algorithm, key, sidSnapshotDate, sidOnMapFailure) match
      case ("sid_mapping", key, snapshotDate, mapFail) =>
        Right(SidMapping(key, snapshotDate, mapFail))
      case ("papis_compatible", key, _, _) => Right(PapisCompatible(key))
      case ("default", key, _, _)          => Right(Default(key))
      case _ =>
        Left(
          DecodingFailure("Failed to decode EncryptionAlgorithm", List.empty)
        )
  yield decode
}

case class PseudoTask(
    name: String,
    columns: List[String],
    encryption: EncryptionAlgorithm
)

given Decoder[HashMap[String, String]] with
  def apply(c: HCursor): Decoder.Result[HashMap[String, String]] =
    for listOfMaps <- c.as[List[Map[String, String]]]
    yield HashMap.from(listOfMaps.flatten)

given Decoder[PseudoTask] = ConfiguredDecoder.derived(using
  Configuration.default.withSnakeCaseMemberNames
)

// Type representing a depseudonymize task
case class DepseudoTask(
    columns: List[String]
)
given Decoder[DepseudoTask] = deriveDecoder

// The type of pseudo task
enum PseudoOperationType:
  case Pseudo(tasks: List[PseudoTask])
  case Depseudo(task: DepseudoTask)

given Decoder[PseudoOperationType] = Decoder.instance { cursor =>
  val tryPseudo = cursor
    .downField("pseudo")
    .as[List[PseudoTask]]
    .map(PseudoOperationType.Pseudo.apply)

  val tryDepseudo = cursor
    .downField("depseudo")
    .as[DepseudoTask]
    .map(PseudoOperationType.Depseudo.apply)

  tryPseudo.orElse(tryDepseudo).left.map { _ =>
    DecodingFailure(
      "Expected either { pseudo: [...] } or { depseudo: {...} }",
      cursor.history
    )
  }
}

case class DelomatenConfig(
    sharedBucket: String,
    sourceFolderPrefix: String,
    destinationFolder: String,
    memorySize: Int,
    operation: PseudoOperationType
)
given Decoder[DelomatenConfig] = Decoder.instance { c =>
  for
    sharedBucket <- c.downField("shared_bucket").as[String]
    sourceFolderPrefix <- c.downField("source_folder_prefix").as[String]
    destinationFolder <- c.downField("destination_folder").as[String]
    memorySize <- c.downField("memory_size").as[Int]
    operation <- summon[Decoder[PseudoOperationType]].apply(c)
  yield DelomatenConfig(
    sharedBucket,
    sourceFolderPrefix,
    destinationFolder,
    memorySize,
    operation
  )
}

// We have to provide custom decoders for the `SharedBuckets` POJO
// because automatic derivation doesn't work for non-scala classes
given Decoder[SharedBucket] with
  def apply(c: HCursor): Decoder.Result[SharedBucket] =
    for
      name <- c.downField("name").as[String]
      typeOpt <- c.downField("type").as[Option[String]]
      sharedWith <- c.downField("sharedWith").as[Option[Set[String]]]
    yield
      val bucket = SharedBucket()
      bucket.setName(name)
      bucket.setType(typeOpt.map(SharedBucket.TypeEnum.fromValue).orNull)
      bucket.setSharedWith(sharedWith.map(_.asJava).orNull)
      bucket

given Decoder[SharedBuckets] with
  def apply(c: HCursor): Decoder.Result[SharedBuckets] =
    for
      versionStr <- c.downField("version").as[String]
      versionEnum <- Right(SharedBuckets.VersionEnum.fromValue(versionStr))
      kindStr <- c.downField("kind").as[String]
      kindEnum <- Right(SharedBuckets.KindEnum.fromValue(kindStr))
      buckets <- c.downField("buckets").as[List[SharedBucket]]
    yield
      val sharedBuckets = SharedBuckets()
      sharedBuckets.setVersion(versionEnum)
      sharedBuckets.setKind(kindEnum)
      sharedBuckets.setBuckets(buckets.asJava)
      sharedBuckets

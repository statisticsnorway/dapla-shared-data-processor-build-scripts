package com.schemavalidation.config.utils

import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*
import io.circe.yaml
import java.nio.file.{Files, Path, Paths}
import scala.io.Source

/** Load YAML configuration and decode it into a scala type.
  */
def loadConfig[A](path: Path)(using
    decoder: Decoder[A]
): Either[ParsingFailure | DecodingFailure, A] =
  val configData = Source.fromFile(path.toFile).getLines().mkString("\n")

  yaml.parser
    .parse(configData)
    .flatMap(_.as(using decoder))

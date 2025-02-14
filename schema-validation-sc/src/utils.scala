package com.schemavalidation.utils

import cats.syntax.all.*
import io.circe.*
import io.circe.syntax.*
import io.circe.yaml
import java.nio.file.{Files, Path, Paths}
import scala.io.Source

/** Load YAML configuration and decode it into a scala type.
  */
def loadConfig[A](path: Path)(using decoder: Decoder[A]): A =
  val configData = Source.fromFile(path.toFile()).getLines().mkString("\n")

  yaml.parser
    .parse(configData)
    .leftMap(err => err: Error)
    .flatMap(_.as(decoder)) match
    case Left(err)     => throw Exception(s"Error: ${err.getMessage()}")
    case Right(config) => config

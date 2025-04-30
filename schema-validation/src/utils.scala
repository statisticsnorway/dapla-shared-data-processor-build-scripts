package com.schemavalidation.utils

// Helper extensions for String type
extension (str: String)
  def red = str.split("\n").map(Console.RED + _).mkString("\n")
  def green = str.split("\n").map(Console.GREEN + _).mkString("\n")
  def yellow = str.split("\n").map(Console.YELLOW + _).mkString("\n")
  def newlines = s"\n\n$str\n\n"

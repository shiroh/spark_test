package citi_test

import com.typesafe.config.Config
import citi.test.processor.FIXConstructor
import citi.test.processor.FIXProcessor

trait Consumer {
  def start()
}

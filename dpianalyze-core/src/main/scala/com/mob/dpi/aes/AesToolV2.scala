package com.mob.dpi.aes

import java.io.{Serializable, UnsupportedEncodingException}
import java.util.Base64

import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import org.apache.commons.lang3.StringUtils

object AesToolV2 extends Serializable {
  // 山东的mode
  val AES_CBC_PKCS5_PADDING = "AES/CBC/PKCS5Padding"
  val AES_CBC_NO_PADDING = "AES/CBC/NoPadding"
  val UTF8 = "UTF-8"
  val AES = "AES"
//  var cipher: Cipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING)
  // 山东的秘钥
  def _key: SecretKeySpec = key128("pwd1234567891234")
  // 山东的偏移量
  def _iv: IvParameterSpec = iv128("iv12345678912345")

  val aesRes: Map[String, Long] = Map.empty[String, Long]

  def encrypt64(data: String): String = {
    try {
      return Base64.getEncoder.encodeToString(encrypt(data.getBytes))
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    ""
  }

  def encrypt(data: String): Array[Byte] = {
    encrypt(data.getBytes)
  }

  def encrypt(data: Array[Byte]): Array[Byte] = {
    try {
      val cipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING)
      cipher.init(Cipher.ENCRYPT_MODE, _key, _iv)
      return cipher.doFinal(data)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
    Array.empty[Byte]
  }

  def decrypt64(base64Data: String): String = {
    try {
      return new String(decrypt(Base64.getDecoder.decode(base64Data)))
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    ""
  }

  def decrypt(encryptedData: Array[Byte]): Array[Byte] = {
    try {
      val cipher = Cipher.getInstance(AES_CBC_PKCS5_PADDING)
      cipher.init(Cipher.DECRYPT_MODE, _key, _iv)
      return cipher.doFinal(encryptedData)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw ex
    }
    Array.empty[Byte]
  }

  def key128(key: String): SecretKeySpec = {
    new SecretKeySpec(byte128(key), AES)
  }

  def iv128(iv: String): IvParameterSpec = {
    new IvParameterSpec(byte128(iv))
  }

  def byte128(value: String): Array[Byte] = {
    var _value = ""
    if (!StringUtils.isEmpty(value)) _value = value
    val buff: StringBuffer = new StringBuffer(16)
    buff.append(_value)

    while (buff.length() < 16) {
      buff.append("0")
    }

    if (buff.length() > 16) {
      buff.setLength(16)
    }

    try {
      buff.toString.getBytes(UTF8)
    } catch {
      case ex: UnsupportedEncodingException =>
        println("========> UnsupportedEncodingException <========")
        throw ex
      case ex: Exception =>
        throw ex
    }
  }
}

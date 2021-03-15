package com.mob.dpi.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="imei_luhn_recomputation",
  value="_FUNC_(String imei14) - Returns the imei15")
public class ReturnImei15 extends UDF {

  public static boolean isNumeric(String str) {
    for (int i = 0; i < str.length(); i++) {
//            System.out.println(str.charAt(i));
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  public static String evaluate(String codeStr) throws Exception {
    String code = codeStr.replaceAll("\\s*","");
    if (code.length() != 14 || !isNumeric(code)) {
      throw new RuntimeException("传入的imei长度超过14 或 为非法字符");
    }
    int total = 0, sum1 = 0, sum2 = 0;
    int temp = 0;
    char[] chs = code.toCharArray();
    for (int i = 0; i < chs.length; i++) {
      int num = chs[i] - '0';    // ascii to num
            /*(1)将奇数位数字相加(从1开始计数)*/
      if (i % 2 == 0) {
        sum1 = sum1 + num;
      } else {
                /*(2)将偶数位数字分别乘以2,分别计算个位数和十位数之和(从1开始计数)*/
        temp = num * 2;
        if (temp < 10) {
          sum2 = sum2 + temp;
        } else {
          sum2 = sum2 + temp + 1 - 10;
        }
      }
    }
    total = sum1 + sum2;
        /*如果得出的数个位是0则校验位为0,否则为10减去个位数 */
    if (total % 10 == 0) {
      return code + "0";
    } else {
      return code + (10 - (total % 10)) + "";
    }
  }
}

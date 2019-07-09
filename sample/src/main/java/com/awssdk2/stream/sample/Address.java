package com.awssdk2.stream.sample;

import lombok.Data;

@Data
public class Address{

  /** 住所コード。プライマリコード */
  private String addressCode;

  /** 郵便番号 */
  private String zipCode;

  /** 都道府県コード */
  private String prefectureCode;

  /** 都道府県 */
  private String prefectureNama;

  /** 市区町村 */
  private String cityName;

  /** 町域 */
  private String districtName;

  /** 字丁目 */
  private String blockName;
}
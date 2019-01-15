# [1.5.1] - 2018-12-29

## add country_state_iso_3166_2_20171123.json
*add state_code_iso_3166_2_20171123 field*
1. 中国 CN
CN-11 → CN-BJ
CN-12 → CN-TJ
CN-13 → CN-HE
CN-14 → CN-SX
CN-15 → CN-NM
CN-21 → CN-LN
CN-22 → CN-JL
CN-23 → CN-HL
CN-31 → CN-SH
CN-32 → CN-JS
CN-33 → CN-ZJ
CN-34 → CN-AH
CN-35 → CN-FJ
CN-36 → CN-JX
CN-37 → CN-SD
CN-41 → CN-HA
CN-42 → CN-HB
CN-43 → CN-HN
CN-44 → CN-GD
CN-45 → CN-GX
CN-46 → CN-HI
CN-50 → CN-CQ
CN-51 → CN-SC
CN-52 → CN-GZ
CN-53 → CN-YN
CN-54 → CN-XZ
CN-61 → CN-SN
CN-62 → CN-GS
CN-63 → CN-QH
CN-64 → CN-NX
CN-65 → CN-XJ
CN-71 → CN-TW
CN-91 → CN-HK
CN-92 → CN-MO

2. 朝鲜 KP
add state_code：14
3. 塔吉克斯坦 TJ
add state_code：RA
4. 卡塔尔 QA
add state_code：SH
5. 马里 ML
add state_code：9、10

## GeoUtil
1. upgrade parsing IP address function, add support for GeoLite2-City.mmdb
2. auto update GeoLite2-City.mmdb every Thursday at 9 a.m.
3. it is compatible to use old GeoIP2-City.mmdb and country_state.json file

## CompressUtil
1. add decompressTarGz function

## FileUtil
1. add writeFile function, which doesn't close inputStream
2. update deleteFile function, add support for delete whole directory
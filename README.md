# nifi
该项目实现了一个自定义的processor，其中实现的主要功能有：对来源数据进行解析，并于redis交互。
需求：
数据格式
源数据格式
{“user_id”：“fasfdsf6s876fs7d6f7ds6f”，“data”：[{“a”:75.0},{“b”,85.2},{“c”,22},{},{},{}]}

返回数据格式
{
“user_id”：“fasfdsf6s876fs7d6f7ds6f”，
“data”：[
{“a”:75.0, “status”:0},
{“b”,85.2, “status”:-1},
{“c”,22, “status”:1},
{},
{},
{}
],
“star”:3
}

等级分析规则：
1. 当重要指标和其他指标都处于合理区间时，为良好（星级为5星）
2. 当重要指标都处于合理区间，其他指标出现3项（包含3项）以内超标，为正常；（星级为4星）
3. 当重要指标都处于合理区间，其他指标出现3项以上超标，也为正常；（星级为3星）
4. 当重要指标有3项（包含3项）以内超出正常范围，其他指标出现3项（包含3项）以内超标，也为正常；（星级为2星）
5. 当重要指标有3项（包含3项）以内超出正常范围，其他指标出现3项以上超标，也为正常；（星级为1星）
6. 当重要指标有3项以上指标超出正常范围，为异常（星级为0星）；
数据地址
mysql
规则数据：10.1.2.49服务器 test库 regulation 表
redis
10.1.24.215服务器 17003端口

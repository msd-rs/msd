/**
 * 将D64格式的BigInt转换为JavaScript浮点数
 * @param {bigint | number | string} d64 - 64位整数，遵循D64格式规范
 * @returns {number} 转换后的浮点数
 */
export function d64ToFloat(d64: bigint | number | string): string {
  // 确保输入为BigInt类型
  if (typeof d64 === "number" || typeof d64 === "string") {
    d64 = BigInt(d64);
  }

  // 定义掩码常量（BigInt）
  const NEG_MASK = 0x1n; // 位0: 0b0001
  const INF_MASK = 0x2n; // 位1: 0b0010
  const NAN_MASK = 0x4n; // 位2: 0b0100
  const SCALE_MASK = 0xf0n; // 位4-7: 0b11110000 (4位)
  const VALUE_MASK = 0xffffffffffffff00n; // 位8-63 (56位)

  if (d64 == 0n) {
    return "0";
  }

  // 1. 优先检查NAN（最高优先级）
  if (d64 & NAN_MASK) {
    return "NaN";
  }

  // 2. 检查INF
  if (d64 & INF_MASK) {
    return d64 & NEG_MASK ? "-Infinity" : "Infinity";
  }

  // 3. 提取原始数值（56位）和小数位数（4位）
  const rawValue = (d64 & VALUE_MASK) >> 8n; // 右移8位获取原始值
  const scaleBigInt = (d64 & SCALE_MASK) >> 4n; // 右移4位获取小数位数
  const scale = Number(scaleBigInt); // 转换为普通数字（0-15）

  // 4. 处理零值（包括-0）
  if (rawValue === 0n) {
    return "0";
  }

  // 5. 提取符号
  const isNegative = (d64 & NEG_MASK) === NEG_MASK;

  // 6. 将原始数值转换为字符串
  const numStr = rawValue.toString();
  let decimalStr;

  // 7. 根据小数位数构造带小数点的字符串
  if (scale === 0) {
    // 无小数位
    decimalStr = numStr;
  } else if (numStr.length <= scale) {
    // 需要前导零（例如：123, scale=5 -> "0.00123"）
    const leadingZeros = "0".repeat(scale - numStr.length);
    decimalStr = `0.${leadingZeros}${numStr}`;
  } else {
    // 插入小数点（例如：12345, scale=2 -> "123.45"）
    const intPart = numStr.slice(0, -scale);
    const fracPart = numStr.slice(-scale);
    decimalStr = `${intPart}.${fracPart}`;
  }

  return isNegative ? "-" + decimalStr : decimalStr;
}

// oxlint-disable-next-line no-unused-vars
function testD64ToFloat() {
  // 123 << 8 | (2 << 4) = 31520
  console.log(d64ToFloat("31520")); // 输出: "1.23"
  // 123 << 8 | (2 << 4) | 1 = 31521
  console.log(d64ToFloat("31521")); // 输出: "-1.23"
  // 1230 << 8 | (3 << 4) = 314928
  console.log(d64ToFloat("314928")); // 输出: "1.230"
  // 1230 << 8 | (3 << 4) | 1 = 314929
  console.log(d64ToFloat("314929")); // 输出: "-1.230"
  // NaN
  console.log(d64ToFloat("4")); // 输出: "NaN"
  // Infinity
  console.log(d64ToFloat("2")); // 输出: "Infinity"
  // -Infinity
  console.log(d64ToFloat("3")); // 输出: "-Infinity"
  console.log(d64ToFloat("0"));
}

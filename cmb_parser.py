from email import policy
from email.parser import BytesParser
from bs4 import BeautifulSoup, Tag
import pandas as pd
import sys
import os


def extract_html_content(html_content, css_selector):
    """
    使用CSS选择器从HTML内容中提取内容

    Args:
        html_content (str): HTML字符串内容
        css_selector (str): CSS选择器路径

    Returns:
        str: 提取到的内容文本
    """
    try:
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, "html.parser")

        # 使用CSS选择器查找元素
        selected_element = soup.select_one(css_selector)

        if selected_element:
            # 返回元素的文本内容
            return selected_element
        else:
            # 抛出异常
            raise ValueError(f"未找到选择器 '{css_selector}' 对应的元素")

    except Exception as e:
        # 抛出异常
        raise ValueError(f"解析HTML时出错: {str(e)}")


def extract_transactions(tag: Tag):
    transactions = []
    # 只遍历直接子<tr>标签（不包含嵌套tbody中的tr）
    for tr in tag.find_all("tr", recursive=False):
        div = tr.find_all("div")
        if len(div) == 7:
            trans_date = div[0].get_text(strip=True)
            post_date = div[1].get_text(strip=True)
            description = div[2].get_text(strip=True)
            amount = div[3].get_text(strip=True).removeprefix("¥\xa0")
            card_tail = div[4].get_text(strip=True)
            country = div[5].get_text(strip=True)
            original_amount = div[6].get_text(strip=True)
            transactions.append({
                "transaction_date": trans_date,
                "posting_date": post_date,
                "description": description,
                "amount": amount,
                "card_tail": card_tail,
                "country": country,
                "original_amount": original_amount
            })
    return transactions


# 检查命令行参数
if len(sys.argv) != 2:
    print("用法: python cmb_parser.py <eml文件路径>")
    print("示例: python cmb_parser.py /path/to/招商银行信用卡电子账单.eml")
    sys.exit(1)

eml_file_path = sys.argv[1]

# 检查文件是否存在
if not os.path.isfile(eml_file_path):
    print(f"错误: 文件不存在: {eml_file_path}")
    sys.exit(1)

# 读取 EML 文件
with open(eml_file_path, "rb") as fp:
    msg = BytesParser(policy=policy.default).parse(fp)

html_body = ""
# 获取邮件HTML正文
if msg.is_multipart():
    for part in msg.walk():
        content_type = part.get_content_type()
        if content_type == "text/html":
            html_body = part.get_payload(decode=True).decode()  # pyright: ignore[reportAttributeAccessIssue]
            break  # 只获取HTML内容
else:
    html_body = msg.get_payload(decode=True).decode()  # pyright: ignore[reportAttributeAccessIssue]

# 使用CSS选择器提取内容
css_selector = "#loopBand2 > table > tbody"
extracted_content = extract_html_content(html_body, css_selector)
transactions = extract_transactions(extracted_content)

# 将交易数据转换为DataFrame并使用中文列名
df = pd.DataFrame(transactions)

# 重命名列为中文
column_mapping = {
    "transaction_date": "交易日",
    "posting_date": "记账日",
    "description": "交易摘要",
    "amount": "人民币金额",
    "card_tail": "卡号末四位",
    "country": "交易地",
    "original_amount": "交易地金额"
}
df = df.rename(columns=column_mapping)

# 只保留中文列
chinese_columns = ["交易日", "记账日", "交易摘要", "人民币金额", "卡号末四位", "交易地", "交易地金额"]
df = df[chinese_columns]

# 保存为XLSX文件
output_file = "cmb_transactions.xlsx"
df.to_excel(output_file, index=False, engine='openpyxl')
print(f"交易数据已保存到: {output_file}")
print(f"共 {len(transactions)} 条交易记录")

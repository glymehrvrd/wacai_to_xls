• 多渠道对账工具设计文档

  - 项目背景与目标
      - 基于现有 wacai.py 导出能力，新增一个支持微信支付 XLSX、支付宝 CSV、中信信用卡 XLSX、招商
        信用卡 EML 的账单整合与对账工具。
      - 自动将多渠道流水清洗、分类、去重后写入与 wacai.xlsx 相同结构的新 Excel，直接可导入挖财或继
        续使用现有流程。
      - 在导入前对照现有 wacai.xlsx，金额重复的记录不再写入；保留来源渠道信息供后续追溯。
  - 范围定义
      - 涵盖支出、收入、转账、借入借出、收款还款五个工作表。
      - 分类依据现有模板列，优先使用大模型对交易备注进行自动归类，无映射时标记“待分类”并在备注中
        提示。
      - 每次运行输出 data/wacai-YYYYMMDDHHMM.xlsx 时间戳文件，包含旧数据 + 新增非重复条目；备注或新增列写入来源渠道，避免覆盖原有记录。
      - 数据源目录：data/，包含四类渠道示例及 wacai-demo.xls、wacai.xlsx 参考模板，其中 wacai.xlsx 为对账基线。
  - 模板字段与中间 Schema
      - wacai.xlsx 各工作表列结构分别为：支出 12 列、收入 10 列、转账 9 列、借入借出 7 列、收款还款 8 列，列名需保持与模板一致。
      - 标准化后的中间结果亦采用 5 张表结构，生成 CSV（建议位置：`intermediate/{channel}/{sheet}.csv`），确保列顺序与模板一致，编码使用 UTF-8-SIG。
      - 若某渠道无对应数据，也要输出仅含表头的空 CSV，保持后续对账流程的统一输入。
  - 用户场景
      - 用户下载各渠道账单放入 data/，执行命令生成带时间戳的新文件（如 wacai-202510121151.xlsx）。
      - 用户可只提供部分渠道文件，工具自动跳过缺失文件。
      - 用户可选择是否启用大模型分类，或提供手工映射表覆盖模型输出。
  - 流程与系统架构
      1. 渠道文件发现（`discover_channel_files` in `pipeline.py`）
          - 扫描 `input-dir`，根据渠道枚举的命名片段（如 wechat、alipay、citic、cmb）匹配文件。
          - 找不到某渠道文件时跳过并在日志中提示，后续流程仅处理已解析的渠道。
      2. 渠道解析与标准化（`parse_channels` + `wacai_reconcile/parsers/*`）
          - 逐渠道调用解析器，将原始账单转为 `StandardRecord` 列表；金额使用 `Decimal`，时间统一为上海时区 `datetime`。
          - 微信、支付宝解析器会过滤银行卡代扣（添加 `non-wallet-payment` 跳过原因），信用卡解析器保留补充信息放入 `meta`。
          - 若配置了 `intermediate-dir`，`write_intermediate_csv` 会按渠道/Sheet 写出 CSV（例如 `intermediate/wechat/支出.csv`）供人工核查。
      3. 基线加载与锁定（`load_wacai_workbook` + `build_account_locks`）
          - 从基线 `wacai.xlsx` 读入各 Sheet DataFrame，缺失的 Sheet 自动补齐空表。
          - 通过 `build_account_locks` 识别备注为“余额调整产生的烂账”或收入大类为“漏记款”的记录，为对应账户计算锁定日期；锁定时间前的交易将被标记为 `account-locked`。
      4. 退款配对与去重链路
          - `apply_refund_pairs`：按配置窗口（默认 30 天）查找金额相反且备注/来源相匹配的记录，命中后两条记为 `canceled`。
          - `BaselineIndex` + `apply_baseline_dedupe`：对比基线数据（金额容差 + 日期容差 + 备注），命中的记录打上 `duplicate-baseline` 跳过原因。
          - `supplement_card_remarks`：若信用卡交易可与钱包交易匹配商户与金额，向备注追加“来源补充(...)”说明退款或商品信息。
      5. 人工确认与导出
          - 根据 `auto_confirm` 参数选择是否逐条提示接受/跳过；自动模式直接接受所有未被标记为 skipped/canceled 的记录。
          - `incremental-only` 为真时仅构建增量帧并输出 CSV；否则使用 `SheetBundle` 将新记录合并到基线数据并按日期升序排序。
          - `write_wacai_workbook` 写出最终 Excel（`{output-prefix}-{timestamp}.xlsx`），同时根据 `report_path` 输出包含所有记录状态的报告 CSV。

  - 详细流程拆解（代码主干逐步说明）
      1. 输入准备
          - CLI（`reconcile.py` → `parse_args`）解析路径、容差、模式开关等参数，组装 `ReconcileOptions`。
          - `ReconcileOptions.resolved_baseline()` 确定基线文件路径，默认 `input-dir/wacai.xlsx`。
      2. 渠道发现与解析
          - `discover_channel_files(options.input_dir)` 返回 `{channel: Path}` 映射；匹配规则为 `*{pattern}*`。
          - `parse_channels(channel_paths)` 依次调用解析器。每条 `StandardRecord` 会写入 `record.meta.channel`、`record.meta.channel_label`，以便后续去重与报告。
          - 解析器行为要点：
              * `parsers/wechat.py`：对 `支付方式` 进行钱包/银行卡判定，银行卡支付标记为 `non-wallet-payment` 并留在报告供对账。
              * `parsers/alipay.py`：同上，同时保留 `花呗`、`余额宝` 等钱包交易。
              * 信用卡解析器（`citic`, `cmb`）保留账单原始入账日期、卡尾号等信息写入 `record.meta.source_extras`。
          - 若设置 `intermediate_dir`，`write_intermediate_csv` 会将非 `supplement_only` 的记录写成 CSV，供人工复核或差异追踪。
      3. 基线加载与账户锁定
          - `load_wacai_workbook(baseline_path)` 载入基线 Excel，确保五个 Sheet 均存在；缺失 sheet 会创建空 DataFrame，并保留列顺序。
          - `build_account_locks(baseline_frames)` 扫描基线数据，对于备注、类别符合锁定条件的记录，以交易时间更新账户锁定表 `{account: latest_locked_datetime}`。
          - `apply_account_locks(all_records, locks)` 遍历增量记录，若交易时间早于锁定时间则打上 `account-locked` 跳过原因。
      4. 退款、去重与补充说明
          - `apply_refund_pairs` 使用金额差<=容差、时间差<=窗口、备注/来源匹配作为判定；匹配成功的支出/收入互相标记为 `canceled`。
          - `BaselineIndex` 以 `(sheet, account, amount, timestamp±容差, remark)` 为 key，`apply_baseline_dedupe` 发现与基线重复即标记为 `duplicate-baseline`。
          - `supplement_card_remarks` 在信用卡记录备注后追加钱包交易的商品/状态信息，使导出的 Excel 可以直接追溯来源。
      5. 交互确认与输出
          - 根据 `auto_confirm` 决定是否逐条输出 `print_record_summary`（包含表名/账户/金额/时间/备注）。
          - 接受的记录标记 `record.meta.accepted = True`，用于报告统计。
          - `incremental-only=True` 时调用 `build_increment_frames` 输出仅包含新增记录的 DataFrame；否则 `SheetBundle.update_from_records` 合并基线 + 新记录。
          - `write_wacai_workbook` 负责落盘 Excel；`write_report`（若指定 `report_path`）输出一份 CSV，记录所有交易的 `status` 和 `reason`。
          - `reconcile()` 返回 `ReconcileResult`（导出路径、accepted/skipped/canceled/pending 计数以及报告路径），供 CLI 或上层流程汇总。

  - 流程示意图

      ```mermaid
      flowchart TD
          A[CLI 参数解析<br/>ReconcileOptions] --> B[发现渠道文件<br/>discover_channel_files]
          B --> C[解析账单为 StandardRecord<br/>parse_channels]
          C --> D[写中间 CSV<br/>write_intermediate_csv?]
          C --> E[载入基线 + 构建锁定<br/>load_wacai_workbook / build_account_locks]
          E --> F[退款配对<br/>apply_refund_pairs]
          F --> G[基线去重<br/>apply_baseline_dedupe]
          G --> H[信用卡备注补充<br/>supplement_card_remarks]
          H --> I[交互确认<br/>auto_confirm / input loop]
          I --> J[构建输出帧<br/>SheetBundle 或 build_increment_frames]
          J --> K[写 Excel<br/>write_wacai_workbook]
          I --> L[生成报告<br/>write_report]
      ```
  - 功能需求明细
      - 支持命令行入口：uv run python reconcile.py --input-dir data --output-prefix data/wacai.
      - 允许传入 --llm 配置（模型名称、API Key、批次大小），或 --no-llm 仅依赖规则。
      - 日志输出导入条目数、跳过条目数（按金额去重）、分类成功率、未匹配分类条目列表。
      - 提供配置文件（如 config/categories.yml）维护固定映射：交易号/关键词→模板分类。
      - 生成后的 Excel 按交易时间正序排序，保持与原始模板一致；保留原币种、汇率信息在备注。
      - 命令行交互模块基于 argparse + 简单输入提示，后续可集成 prompt-toolkit。
      - 支持 `--report-path` 自定义导入报告输出目录。
      - 输出文件命名规则：根据执行时间生成 `"{output-prefix}-{YYYYMMDDHHMM}.xlsx"`，保留历史版本，不覆盖既有文件。
      - 支持 `--account-lock` 相关设置，控制遇到余额调整或漏记款时的锁定行为（默认启用）。
      - `--incremental-only` 仅输出增量交易，便于先复核新增条目再决定合并。
      - 支持退款匹配配置（如 `--refund-window 30d`），控制金额相反且备注一致的支出/退款自动抵消逻辑，默认开启并输出匹配日志。
      - 支付宝渠道仅保留余额/余额宝/花呗等自有账户流水，过滤银行卡代扣记录，避免与信用卡账单重复。
      - 对银行信用卡账单补充备注：若能在微信/支付宝中匹配到同金额、同商户的记录，则附加来源备注（含退款状态）用于追溯。
  - 非功能要求
      - 解析与分类模块解耦，便于未来新增渠道。
      - 处理 10 万级别交易记录的性能需在 5 分钟内完成。
      - 对异常文件（空文件、格式错误）给出明确错误提示，不影响其他渠道处理。
      - 遵循当前仓库编码规范（PEP 8，四空格缩进，UTF-8）。
  - 开放问题与假设
      - 金额一致即认定重复，可能误删不同日期的同额交易；需在文档中提示用户风险。
      - 大模型调用依赖外部网络及权限，默认提供接口占位与本地缓存机制，需用户自配 API Key。
      - 若未来需要更精细的重复判定（时间/交易号），将作为后续迭代。
      - 余额调整/漏记款锁定策略依赖备注及大类文本匹配，若模板字段变化需同步更新规则。
      - 退款抵消依赖金额与备注精确匹配，若上游渠道备注存在差异需提供自定义映射或人工确认。
  - 实施阶段建议
      1. M1：基础解析与输出
          - 实现四渠道解析器，将各渠道账单标准化为 5 张 CSV（支出/收入/转账/借入借出/收款还款）并写入中间目录；补齐 data/ 下最小化示例。
          - 定义判重规则（金额 + 日期差阈值 + 渠道/原始 ID）并支持 dry-run。
      2. M2：去重与分类增强
          - 完成金额去重逻辑、分类失败提示、来源渠道写入；输出对账日志和导入报告。
          - 引入可调判重策略（金额相同但日期差大时提示复核），实现支出与退款抵消规则及锁定日志。
      3. M3：大模型接入
          - 集成 LLM 分类接口、缓存策略、失败回退。
      4. M4：体验优化
          - 增加命令行参数、生成导入报告、引导用户配置映射表。
  - 风险与缓解
      - 分类准确率不稳定：引入可编辑映射表 + 人工二次确认列表。
      - 金额重复误判：日志列出被剔除交易（含日期、渠道），允许用户手工恢复。
      - 外部模型依赖：提供纯规则模式，避免网络受限导致分类失败。
      - 模板字段变化：建立单元测试对照 wacai-demo.xls，确保生成结构一致。

  该设计文档可作为后续开发、评审和排期的基础。若有新增需求或调整，请继续补充，我会同步更新。

  - 测试与验收标准
      - 单元测试
          - `tests/parsers/`：针对微信、支付宝、信用卡解析器构造最小化账单样例，断言输出 `StandardRecord` 字段（金额、时间、渠道标识、跳过原因）。
          - `tests/pipeline/test_dedupe.py`：构造基线与增量组合，验证 `duplicate-baseline`、`channel-duplicate`、`account-locked` 标记逻辑。
          - `tests/utils/`：覆盖 `to_decimal`、`normalize_text`、`as_datetime` 等基础工具函数边界情况（空字符串、异常格式、非 ASCII 文本）。
      - 集成测试
          - 提供匿名化多渠道夹具（含退款、银行卡代扣、账户锁定场景），执行 `uv run python reconcile.py --input-dir tests/fixtures --output-prefix build/test --auto-confirm --dry-run --report-path build/report.csv`。
          - 断言输出报告中 accepted/skipped/canceled 数量与预期一致，验证 wallet-card 去重与退款抵消是否生效。
      - 端到端验收
          - 使用真实模板 `wacai.xlsx` 复制品作为基线，执行一次完整流程（不带 `--dry-run`），人工检查生成的 Excel 与基线列结构一致，日期排序正确。
          - 打开导出文件，对比关键示例：支付宝银行卡支付应在报告中标记为 `non-wallet-payment`，中信信用卡对应条目备注包含“来源补充(支付宝/微信)”，退款记录被标记为 `canceled`。
          - 验证 CLI 交互路径：在未开启 `--auto-confirm` 下，人工选择“跳过”“全部导入”“退出”等分支，确保流程无异常。
      - 性能验收
          - 在包含 10 万条交易的夹具上执行全流程，记录总耗时；要求在 MacBook Pro（Apple M 系列/32GB RAM）上 5 分钟以内完成。

  codex resume 0199d365-b2e0-77a1-a4ab-fbf3da3854cd

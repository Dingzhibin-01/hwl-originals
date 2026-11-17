# candidates/ —— 待验证的信息源清单

本目录存放"候选 RSS/Atom 源"清单。`verify-feeds` 工作流（见仓库根目录 `VERIFY.md`）
按批次切分这里的 `candidates.csv`，用 GitHub Actions 的机器逐条探测可达性与内容新鲜度。

## 文件

| 文件 | 作用 |
| --- | --- |
| `candidates.csv` | **正式清单**，工作流默认读取它。由上游清单生成流程产出，可能上万行。 |
| `sample.csv` | 20 条真实可用的示例源，用于冒烟测试（`batch=1 chunk=20`）。 |

`candidates.csv` 目前可能尚未提交（由另一个流程生成后覆盖），只有 `sample.csv` 时可先跑
`batch=1 chunk=20` 验证工作流本身。

## candidates.csv 格式

UTF-8 文本，逗号分隔，**第一行必须是表头**，列名固定为：

```
url,domain,kind,country,lang
```

| 列 | 必填 | 说明 |
| --- | --- | --- |
| `url` | 是 | 源的绝对地址，必须以 `http://` 或 `https://` 开头，不能有空格。 |
| `domain` | 否 | 站点注册域名，如 `state.gov`、`theguardian.com`（去掉 `www.`）。留空时脚本自动从 `url` 提取 host。 |
| `kind` | 否 | 源类型，枚举：`news`（媒体）、`gov`（政府/使馆）、`igorg`（国际组织）、`ngo`、`thinktank`（智库）、`data`（数据/监测）。 |
| `country` | 否 | ISO 3166-1 两位国家码大写（`US`、`GB`、`JP`）；多国组织填 `GLOBAL`，欧盟填 `EU`；港澳台等地区可填 `HK`、`TW`。 |
| `lang` | 否 | ISO 639-1 语言码：`en`、`zh`、`ja`、`ko`、`ar`、`fr`、`de`、`ru`、`es`。 |

### 硬性约定

1. **不要写注释行、不要写空行**。脚本会跳过空行，但 `#` 开头的行会被当成一条 URL 去探测。
2. 字段里含逗号、双引号时必须使用标准 CSV 转义（整段用双引号包起来，内部 `"` 写成 `""`）。
   URL 里常见的 `?a=1&b=2` 不需要转义。
3. 一行一个源，**行序即批次序**：第 N 批 = 第 `(N-1)*chunk+1` 行到 `N*chunk` 行（不含表头）。
   所以重新排序会改变分批结果，定稿后尽量别再动。
4. 同一 URL 不要重复出现，否则会重复消耗探测配额。
5. 单批上限 800 条（工作流会拒绝更大的 `chunk`）。

### 示例

```csv
url,domain,kind,country,lang
https://www.state.gov/rss-feed/press-releases/feed/,state.gov,gov,US,en
https://www.theguardian.com/world/rss,theguardian.com,news,GB,en
https://www.crisisgroup.org/rss,crisisgroup.org,thinktank,BE,en
```

## 数量与批次

`总批数 = ceil(总行数 / chunk)`。按默认 `chunk=500`：

| 行数 | 批数 |
| --- | --- |
| 500 | 1 |
| 5,000 | 10 |
| 10,000 | 20 |
| 16,000 | 32（超过 batch 输入上限 20，需要提高 chunk 或分多次清单） |

工作流的 `batch` 输入限定 1–20，`chunk` 最大 800，即单份清单最多覆盖
`20 × 800 = 16,000` 行。超过这个量级请拆成多份清单或调大 `chunk`（并重新核对单批耗时）。

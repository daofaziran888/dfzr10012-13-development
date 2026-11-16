import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/checkpoint'
const columns = [
  "记录编号",
  "流作业名",
  "检查点ID",
  "状态大小",
  "保存路径",
  "创建时间",
  "恢复耗时",
  "记录状态",
  "目标实例",
  "恢复锁"
]
const actions = [
  "恢复状态",
  "清理记录",
  "归档记录"
]
const statuses = [
  "已创建",
  "已恢复",
  "已过期",
  "已清理"
]
const manifestColumns = [
  "恢复请求号",
  "记录编号",
  "检查点ID",
  "目标实例",
  "保存路径",
  "状态大小",
  "恢复耗时",
  "状态",
  "恢复锁",
  "恢复锁状态",
  "路径引用"
]
const manifestActions = [
  "开始恢复",
  "完成恢复",
  "归档恢复",
  "恢复失败"
]

type Row = Record<string, string | number | null>
type Manifest = Record<string, string | number | null>

export default function CheckpointView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [manifests, setManifests] = useState<Manifest[]>([])
  const [manifestTotal, setManifestTotal] = useState(0)
  const [manifestStatus, setManifestStatus] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('检查点记录列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '检查点恢复列表读取失败')
    }
  }, [filters])

  const reloadManifests = useCallback(async () => {
    const query = manifestStatus ? `?status=${encodeURIComponent(manifestStatus)}` : ''
    try {
      const response = await request(`${ENDPOINT}/manifests${query}`)
      if (!response.ok) {
        throw new Error('恢复数据清单读取失败')
      }
      const payload = await response.json()
      setManifests(payload.items ?? [])
      setManifestTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '恢复数据清单读取失败')
    }
  }, [manifestStatus])

  useEffect(() => {
    void reload()
  }, [reload])

  useEffect(() => {
    void reloadManifests()
  }, [reloadManifests])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    setNotice('')
    if (action === '恢复状态') {
      await startRestore(row)
      return
    }
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok || payload.ok === false) {
        throw new Error(String(payload.message ?? '检查点恢复动作未生效，请稍后重试'))
      }
      setNotice(String(payload.message ?? '检查点记录已更新'))
      await Promise.all([reload(), reloadManifests()])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '检查点恢复操作失败')
    }
  }

  async function startRestore(row: Row) {
    const requestNo = window.prompt(
      `为检查点 ${row['检查点ID'] ?? ''}（保存路径 ${row['保存路径'] ?? '—'}）登记恢复请求号`,
      `restore-${String(row['检查点ID'] ?? row.id)}`,
    )
    if (requestNo === null) return
    const restoreSeconds = window.prompt('恢复耗时（秒）', String(row['恢复耗时'] ?? '42'))
    if (restoreSeconds === null) return
    try {
      const response = await request(`${ENDPOINT}/${row.id}/restore`, {
        method: 'POST',
        body: JSON.stringify({ 恢复请求号: requestNo, 恢复耗时: restoreSeconds }),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok || payload.ok === false) {
        throw new Error(String(payload.message ?? '恢复条件未通过，未写入恢复清单'))
      }
      const manifest = payload.entry?.manifest
      setNotice(
        manifest
          ? `${payload.message}；目标实例 ${manifest['目标实例']}，恢复耗时 ${manifest['恢复耗时']} 秒，恢复锁 ${manifest['恢复锁']}`
          : String(payload.message ?? '检查点恢复已受理'),
      )
      await Promise.all([reload(), reloadManifests()])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '检查点恢复发起失败')
    }
  }

  async function runManifestAction(action: string, manifest: Manifest) {
    setErrorMessage('')
    setNotice('')
    const lock = String(manifest['恢复锁'] ?? '')
    try {
      const body: Record<string, string> = { action }
      if (action === '恢复失败') {
        const reason = window.prompt('说明恢复失败原因，清单与保存路径引用会一起退回', '目标实例启动超时')
        if (reason === null) return
        body.reason = reason
      }
      const response = await request(`${ENDPOINT}/manifests/${encodeURIComponent(lock)}/actions`, {
        method: 'POST',
        body: JSON.stringify(body),
      })
      const payload = await response.json().catch(() => ({}))
      if (!response.ok || payload.ok === false) {
        throw new Error(String(payload.message ?? '恢复清单动作未生效'))
      }
      setNotice(String(payload.message ?? '恢复清单已更新'))
      await Promise.all([reload(), reloadManifests()])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '恢复清单操作失败')
    }
  }

  return (
    <section className="page" data-module="checkpoint">
      <header className="page-head">
        <div>
          <h2>检查点恢复管理</h2>
          <p className="page-desc">维护检查点的状态快照、路径管理与恢复计时，为检查点恢复提供可验证的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('检查点记录登记入口尚未接入审批流')}>
            登记检查点记录
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出检查点恢复清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {[
          { label: '检查点记录总数', value: total },
          { label: '恢复清单总数', value: manifestTotal },
          { label: '待处理恢复', value: manifests.filter((item) => item['状态'] === '待处理').length },
        ].map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无检查点恢复数据，可先登记检查点记录</td>
            </tr>
          )}
        </tbody>
      </table>

      <header className="page-head">
        <div>
          <h3>检查点恢复数据清单</h3>
          <p className="page-desc">同一检查点只发放一把恢复锁；恢复失败时清单与保存路径引用一起退回。</p>
        </div>
        <div className="page-actions">
          <select value={manifestStatus} onChange={(event) => setManifestStatus(event.target.value)}>
            <option value="">全部状态</option>
            {['待处理', '运行中', '已恢复', '已归档', '已回退'].map((status) => (
              <option key={status} value={status}>{status}</option>
            ))}
          </select>
        </div>
      </header>

      <table className="data-table">
        <thead>
          <tr>
            {manifestColumns.map((column) => <th key={column}>{column}</th>)}
            <th>清单动作</th>
          </tr>
        </thead>
        <tbody>
          {manifests.map((manifest) => (
            <tr key={String(manifest['恢复锁'])}>
              {manifestColumns.map((column) => (
                <td key={column}>
                  {column === '恢复耗时'
                    ? `${manifest[column] ?? '—'} ${manifest['恢复耗时单位'] ?? '秒'}`
                    : manifest[column] ?? '—'}
                </td>
              ))}
              <td className="row-actions">
                {manifestActions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runManifestAction(action, manifest)}>
                    {action}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {manifests.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={manifestColumns.length + 1}>暂无恢复数据清单，先从检查点记录发起恢复</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>共 {total} 条检查点恢复记录 / {manifestTotal} 条恢复数据清单 · 状态口径：{statuses.join('、')}</span>
        {notice && <span>{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}

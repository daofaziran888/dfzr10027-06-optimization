<template>
  <section class="page" data-module="agg_task">
    <header class="page-head">
      <div><h2>聚合任务</h2><p class="page-desc">维护定时聚合计算任务的配置、依赖关系和执行调度；时间线按真实运行事件流水回放，开始/写入/结束记录均可定位到具体运行。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记聚合计算</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div
            v-if="item.start && item.end"
            class="timeline-bar"
            :class="`status-${item.status}`"
            :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }"
            :data-testid="`bar-${item.id}`"
            :title="`${item.run_id}｜${item.start} ~ ${item.end}`"
            @click="selectItem(item)"
          >&nbsp;</div>
          <span v-else class="timeline-idle" :data-testid="`idle-${item.id}`">待调度（尚无运行事件）</span>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">
          {{ item.start && item.end ? `${item.start} ~ ${item.end}` : '—' }}
          <em v-if="item.run_id" style="font-style:normal;color:var(--ink)">{{ item.run_id }}</em>
        </span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div v-if="selectedItem.run_id" style="margin-top:4px;color:var(--muted)">
        当前运行：{{ selectedItem.run_id }}
        <template v-if="selectedItem.recovered_from">（自 {{ selectedItem.recovered_from}} 失败恢复）</template>
      </div>
      <div v-if="selectedItem.pending_candidate" style="margin-top:6px;padding:6px 8px;background:#fff7e6;border-radius:6px;">
        等待确认候选：运行 {{ selectedItem.pending_candidate.run_id }}（{{ selectedItem.pending_candidate.state }}），
        值 {{ JSON.stringify(selectedItem.pending_candidate.metrics) }}；查询口暂不发布该版本。
      </div>
      <div v-if="selectedItem.published_snapshots?.length" style="margin-top:6px;">
        已发布快照：
        <span v-for="snap in selectedItem.published_snapshots" :key="`${snap.metric}-${snap.version_label}`" style="margin-right:12px;">
          {{ snap.metric }}={{ snap.value }}（{{ snap.version_label }} / {{ snap.run_id }}）
        </span>
      </div>
      <pre style="margin-top:6px;white-space:pre-wrap;font-family:inherit;">{{ selectedItem.logs || '暂无日志' }}</pre>
    </div>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记聚合计算</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <label class="create-field"><span>指标清单(逗号分隔)</span><input v-model="createValues.metrics" placeholder="如 gmv,orders" /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/agg_task'
const columns = ["任务编号", "目标指标", "聚合维度", "计算周期", "依赖上游", "并行度", "超时分钟", "重试上限"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
function scheduledItems() { return allItems.value.filter(i => i.start && i.end) }
function barLeft(item: any) { const all = scheduledItems(); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = scheduledItems(); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => !i.start || i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => !i.end || i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>

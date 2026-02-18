<script setup>
import { onMounted, computed, ref, watch } from 'vue'
import OperatorsTable from '../components/OperatorsTable.vue'
import KPIBox from '../components/KPIBox.vue'
import ExpensesChart from '../components/ExpensesChart.vue'
import { useEstatisticas } from '../composables/useEstatisticas'

const { statistics, loading, fetchStatistics } = useEstatisticas()
const showColdStartMessage = ref(false)
let currentTimeout = null

watch(loading, (newVal) => {
  if (newVal) {
    currentTimeout = setTimeout(() => {
      showColdStartMessage.value = true
    }, 5000) // 5 seconds threshold
  } else {
    clearTimeout(currentTimeout)
    showColdStartMessage.value = false
  }
})

onMounted(() => {
  fetchStatistics()
})

const formatCurrency = (value) => {
  if (value === undefined || value === null) return 'R$ 0'
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', notation: 'compact', maximumFractionDigits: 1 }).format(value)
}

const topOperatorName = computed(() => {
  return statistics.value.top_5_operators?.[0]?.razao_social || 'N/A'
})

const topOperatorValue = computed(() => {
  return formatCurrency(statistics.value.top_5_operators?.[0]?.total_expenses)
})
</script>

<template>
  <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
    <div class="flex justify-between items-center mb-8">
      <h1 class="text-3xl font-bold text-gray-900 tracking-tight">ANS Healthcare Analytics</h1>
    </div>

    <!-- Cold Start Message -->
    <div v-if="showColdStartMessage" class="bg-blue-50 border-l-4 border-blue-400 p-4 mb-8 rounded-r">
      <div class="flex">
        <div class="flex-shrink-0">
          <svg class="h-5 w-5 text-blue-400" viewBox="0 0 20 20" fill="currentColor">
            <path fill-rule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm1-12a1 1 0 10-2 0v4a1 1 0 00.293.707l2.828 2.829a1 1 0 101.415-1.415L11 9.586V6z" clip-rule="evenodd" />
          </svg>
        </div>
        <div class="ml-3">
          <p class="text-sm text-blue-700">
            Loading data... 
            <span class="font-medium">
               (Since we use a free server, it might be waking up. This takes about 50 seconds!) ☕
            </span>
          </p>
        </div>
      </div>
    </div>

    <!-- KPI Cards -->
    <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      <KPIBox 
        title="Total Market Expenses" 
        :value="formatCurrency(statistics.total_expenses)" 
      />
      <KPIBox 
        title="Average Expenses" 
        :value="formatCurrency(statistics.average_expenses)" 
      />
      <KPIBox 
        title="Top Operator Leader" 
        :value="topOperatorName" 
        :subtitle="topOperatorValue"
      />
    </div>
    
    <!-- Chart -->
    <div class="mb-8" v-if="statistics.expenses_by_uf && statistics.expenses_by_uf.length > 0">
      <ExpensesChart :data="statistics.expenses_by_uf" />
    </div>

    <!-- Table -->
    <OperatorsTable />
  </div>
</template>

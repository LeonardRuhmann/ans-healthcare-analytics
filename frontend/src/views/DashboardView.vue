<script setup>
import { onMounted, computed } from 'vue'
import OperatorsTable from '../components/OperatorsTable.vue'
import KPIBox from '../components/KPIBox.vue'
import ExpensesChart from '../components/ExpensesChart.vue'
import { useEstatisticas } from '../composables/useEstatisticas'

const { statistics, loading, fetchStatistics } = useEstatisticas()

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

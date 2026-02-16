<script setup>
import { onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useDetalhes } from '../composables/useDetalhes'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import Button from 'primevue/button'
import { ArrowLeftIcon } from 'lucide-vue-next'

const route = useRoute()
const router = useRouter()
const { operator, expenses, loading, error, fetchDetails } = useDetalhes()

onMounted(() => {
  fetchDetails(route.params.cnpj)
})

const formatCurrency = (value) => {
  if (value === undefined || value === null) return 'R$ 0,00'
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(value)
}

const formatCNPJ = (cnpj) => {
  if (!cnpj) return ''
  return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5")
}
</script>

<template>
  <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
    <button 
      @click="router.back()" 
      class="mb-6 flex items-center text-gray-500 hover:text-gray-900 transition-colors"
    >
      <ArrowLeftIcon class="w-4 h-4 mr-1" />
      Back to Dashboard
    </button>

    <div v-if="loading && !operator" class="text-center py-12">
      <div class="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mx-auto"></div>
    </div>

    <div v-else-if="error" class="bg-red-50 text-red-700 p-4 rounded-lg">
      {{ error }}
    </div>

    <div v-else-if="operator">
      <!-- Operator Header -->
      <div class="bg-white p-6 rounded-lg shadow-sm mb-8 border-l-4 border-blue-500">
        <h1 class="text-2xl font-bold text-gray-900 mb-2">{{ operator.razao_social }}</h1>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-gray-600">
          <div>
            <span class="font-medium text-gray-900">CNPJ:</span> {{ formatCNPJ(operator.cnpj) }}
          </div>
          <div>
            <span class="font-medium text-gray-900">Registro ANS:</span> {{ operator.reg_ans }}
          </div>
          <div v-if="operator.uf">
            <span class="font-medium text-gray-900">State (UF):</span> {{ operator.uf }}
          </div>
        </div>
      </div>

      <!-- Expense History -->
      <div class="bg-white p-6 rounded-lg shadow-sm">
        <h2 class="text-xl font-bold text-gray-900 mb-4">Expense History (Latest Quarter)</h2>
        
        <DataTable 
          :value="expenses" 
          paginator 
          :rows="10" 
          tableStyle="min-width: 50rem"
          stripedRows
          pt:header:class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wider"
        >
          <Column field="data_trimestre" header="Reference Date" sortable></Column>

          <Column field="vl_saldo_final" header="Value (R$)" sortable>
            <template #body="slotProps">
              <span class="font-mono">{{ formatCurrency(slotProps.data.vl_saldo_final) }}</span>
            </template>
          </Column>
        </DataTable>
      </div>
    </div>
  </div>
</template>

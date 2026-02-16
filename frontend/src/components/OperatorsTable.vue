<script setup>
import { onMounted } from 'vue'
import { useOperadoras } from '../composables/useOperadoras'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import Button from 'primevue/button'
import InputText from 'primevue/inputtext'
import { useRouter } from 'vue-router'
import { EyeIcon } from 'lucide-vue-next'

const router = useRouter()
const { operadoras, pagination, filters, loading, fetchOperadoras } = useOperadoras()

onMounted(() => {
  fetchOperadoras()
})

const onPage = (event) => {
  pagination.value.page = event.page + 1
  pagination.value.limit = event.rows
  fetchOperadoras()
}

const formatCNPJ = (cnpj) => {
  if (!cnpj) return ''
  return cnpj.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5")
}
</script>

<template>
  <div class="bg-white p-6 rounded-lg shadow-sm">
    <div class="flex justify-between items-center mb-6">
      <h2 class="text-xl font-bold text-gray-800">Registered Operators</h2>
      <div class="relative">
        <InputText v-model="filters.search" placeholder="Search by CNPJ or Name..." class="pl-10 w-80" />
      </div>
    </div>

    <DataTable 
      :value="operadoras" 
      :loading="loading"
      lazy
      paginator
      :rows="pagination.limit"
      :totalRecords="pagination.total"
      @page="onPage"
      tableStyle="min-width: 50rem"
      pt:header:class="bg-gray-50 text-xs font-semibold text-gray-500 uppercase tracking-wider"
    >
      <Column field="cnpj" header="REG ANS (CNPJ)">
        <template #body="slotProps">
          <div>
            <span class="font-medium text-gray-900">{{ slotProps.data.reg_ans }}</span>
            <span class="text-gray-500 text-sm ml-2">({{ formatCNPJ(slotProps.data.cnpj) }})</span>
          </div>
        </template>
      </Column>
      <Column field="razao_social" header="RAZÃO SOCIAL (COMPANY NAME)"></Column>
      <Column header="ACTIONS" style="width: 15rem">
        <template #body="slotProps">
          <button 
            @click="router.push(`/operator/${slotProps.data.cnpj}`)"
            class="text-blue-600 hover:text-blue-800 font-medium flex items-center text-sm"
          >
            <EyeIcon class="w-4 h-4 mr-1" />
            View Details
          </button>
        </template>
      </Column>
    </DataTable>
  </div>
</template>

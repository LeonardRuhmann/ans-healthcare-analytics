import { ref } from 'vue'
import api from '../services/api'

export function useDetalhes() {
    const operator = ref(null)
    const expenses = ref([])
    const loading = ref(false)
    const error = ref(null)

    const fetchDetails = async (cnpj) => {
        loading.value = true
        error.value = null
        try {
            // Fetch operator details
            const operatorRes = await api.get(`/operadoras/${cnpj}`)
            operator.value = operatorRes.data

            // Fetch expenses history
            const expensesRes = await api.get(`/operadoras/${cnpj}/despesas`)
            expenses.value = expensesRes.data
        } catch (err) {
            console.error(err)
            if (err.response?.data?.detail) {
                error.value = err.response.data.detail
            } else {
                error.value = 'Failed to load operator details.'
            }
        } finally {
            loading.value = false
        }
    }

    return {
        operator,
        expenses,
        loading,
        error,
        fetchDetails
    }
}

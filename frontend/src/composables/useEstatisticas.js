import { ref } from 'vue'
import api from '../services/api'

export function useEstatisticas() {
    const statistics = ref({
        total_expenses: 0,
        average_expenses: 0,
        top_5_operators: [],
        expenses_by_uf: []
    })
    const loading = ref(false)
    const error = ref(null)

    const fetchStatistics = async () => {
        loading.value = true
        error.value = null
        try {
            const response = await api.get('/estatisticas')
            statistics.value = response.data
        } catch (err) {
            console.error(err)
            error.value = 'Failed to load statistics.'
        } finally {
            loading.value = false
        }
    }

    return {
        statistics,
        loading,
        error,
        fetchStatistics
    }
}
